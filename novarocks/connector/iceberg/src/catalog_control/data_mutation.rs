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

//! Generation-local Iceberg implementation of the connector data-mutation contract.
//!
//! Planning, commit dispatch, and reconciliation use only the exact provider
//! runtime supplied by the control factory. No catalog-name registry or
//! process-global async runtime participates in this path.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use crate::iceberg::{NamespaceIdent, TableIdent};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use novarocks_spi::connector::{
    ConnectorDataMutation, ConnectorDataMutationExecuteRequest, ConnectorDataMutationOperation,
    ConnectorDataMutationPlan, ConnectorDataMutationPlanSummary,
    ConnectorDataMutationPlanningRequest, ConnectorDataMutationReceipt,
    ConnectorDataMutationReconcileRequest, ConnectorError, ConnectorErrorKind,
    ConnectorExecutionBindingKey, ConnectorExternalFenceReceipt, ConnectorExternalFenceRequest,
    ConnectorInstanceDescriptor, ConnectorMutationFailure, ConnectorMutationFailureKind,
    ConnectorMutationOperationId, ExternalMutationEffect, ExternalMutationEvidence,
    ExternalMutationFinalization, ExternalMutationOutcome,
};

use super::add_files::{
    AddFilesManifest, plan_manifest_for_table, preflight_caller_managed_source_domain,
    revalidate_manifest_for_table,
};
use crate::commit::write_control::{
    encode_fence_receipt_payload, fence_failure_to_connector_error,
};
use crate::commit::write_fence::{IcebergFenceAssertion, IcebergWriteFenceFacts};
use crate::commit::{
    CleanupAttempt, CleanupPathMapper, CommitServiceError, IcebergCommitCollector,
    RecoveryEvidence, RunInput, run_iceberg_commit,
};
use crate::commit::{CommitOpKind, CommitOutcome, WrittenFile};
use crate::control_provider::IcebergControlProvider;
use crate::control_runtime::IcebergControlRuntime;
use crate::fs_io;

const PLAN_PAYLOAD_VERSION: u16 = 1;
const RECEIPT_PAYLOAD_VERSION: u16 = 1;
const EVIDENCE_PAYLOAD_VERSION: u16 = 1;
const MARKER_VALUE_VERSION: u16 = 1;
const TRUNCATE_OPERATION_KIND: &str = "truncate";
const MAX_DURABLE_TRUNCATE_EVIDENCE_HEX_BYTES: usize = 16 * 1024;
pub(crate) const MAX_DURABLE_ICEBERG_TRUNCATE_EVIDENCE_WIRE_BYTES: usize =
    MAX_DURABLE_TRUNCATE_EVIDENCE_HEX_BYTES / 2;
const MAX_DURABLE_ICEBERG_TRUNCATE_RECEIPT_PROVIDER_PAYLOAD_BYTES: usize = 64;
const MARKER_PROPERTY: &str = "novarocks.connector.data-mutation.v1";
const IDENTITY_DIGEST_DOMAIN: &[u8] = b"novarocks.iceberg.data-mutation.identity.v1\0";
const TRUNCATE_STATE_DIGEST_DOMAIN: &[u8] = b"novarocks.iceberg.data-mutation.truncate-state.v1\0";
const METADATA_VERSION_DIGEST_DOMAIN: &[u8] =
    b"novarocks.iceberg.data-mutation.metadata-version.v1\0";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergDataMutationPlanPayloadV1 {
    version: u16,
    namespace: String,
    table: String,
    table_uuid: String,
    target_ref: String,
    base_snapshot_id: Option<i64>,
    schema_id: i32,
    default_spec_id: i32,
    metadata_version_digest_hex: String,
    source_location: Option<String>,
    name_mapping_digest_hex: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergDataMutationReceiptV1 {
    version: u16,
    snapshot_id: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergDataMutationEvidenceV1 {
    version: u16,
    namespace: String,
    table: String,
    target_ref: String,
    operation_id_hex: String,
    operation_kind: String,
    request_digest_hex: String,
    plan_digest_hex: String,
    state_digest_hex: String,
    identity_digest_hex: String,
    file_count: u32,
    row_count: u64,
    total_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergDataMutationMarkerV1 {
    version: u16,
    identity_digest_hex: String,
    incarnation_hex: String,
    operation_id_hex: String,
    operation_kind: String,
    request_digest_hex: String,
    plan_digest_hex: String,
    state_digest_hex: String,
    target_ref: String,
    base_snapshot_id: Option<i64>,
    file_count: u32,
    row_count: u64,
    total_bytes: u64,
}

#[derive(Clone)]
enum PlannedIcebergMutation {
    RegisterExistingFiles {
        payload: IcebergDataMutationPlanPayloadV1,
        manifest: AddFilesManifest,
        domain: novarocks_spi::connector::ConnectorDataMutationAddFilesDomain,
    },
    Truncate {
        payload: IcebergDataMutationPlanPayloadV1,
    },
}

impl PlannedIcebergMutation {
    fn payload(&self) -> &IcebergDataMutationPlanPayloadV1 {
        match self {
            Self::RegisterExistingFiles { payload, .. } | Self::Truncate { payload } => payload,
        }
    }
}

#[derive(Clone)]
struct CachedPlan {
    request_digest: [u8; 32],
    plan: ConnectorDataMutationPlan,
    private: PlannedIcebergMutation,
}

#[derive(Clone)]
struct TerminalRecord {
    plan_digest: [u8; 32],
    outcome: ExternalMutationOutcome<ConnectorDataMutationReceipt>,
}

trait IcebergDataMutationBackend: Send + Sync {
    /// Publish this attempt's fence marker so that a later execute can assert
    /// it atomically inside the same catalog update that mutates the table.
    ///
    /// There is deliberately no "remember the fence locally" variant: the
    /// marker is the fence. `execute` re-derives its assertion from external
    /// truth, so a backend that acknowledged a fence without publishing one
    /// would make every fenced direct mutation fail closed at commit time.
    fn establish_fence(
        &self,
        facts: &IcebergWriteFenceFacts,
    ) -> Result<IcebergFenceAssertion, ConnectorError>;

    fn plan(
        &self,
        request: &ConnectorDataMutationPlanningRequest,
    ) -> Result<
        (
            PlannedIcebergMutation,
            [u8; 32],
            ConnectorDataMutationPlanSummary,
        ),
        ConnectorError,
    >;

    #[allow(clippy::result_large_err)]
    fn execute(
        &self,
        planned: &PlannedIcebergMutation,
        marker: &IcebergDataMutationMarkerV1,
        fencing: &novarocks_spi::connector::ConnectorWriteFencing,
    ) -> Result<CommitOutcome, CommitServiceError>;

    fn lookup_marker(
        &self,
        namespace: &str,
        table: &str,
        target_ref: &str,
        operation_id_hex: &str,
        identity_digest_hex: &str,
    ) -> Result<MarkerLookup, ConnectorError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MarkerLookup {
    Matching { snapshot_id: i64 },
    Conflicting,
    Missing,
}

struct RegisteredIcebergDataMutationBackend {
    provider: Arc<IcebergControlProvider>,
    runtime: Arc<IcebergControlRuntime>,
}

impl RegisteredIcebergDataMutationBackend {
    fn new(provider: Arc<IcebergControlProvider>) -> Self {
        Self {
            runtime: Arc::clone(provider.runtime()),
            provider,
        }
    }

    fn reload_table(
        &self,
        namespace: &str,
        table: &str,
    ) -> Result<crate::iceberg::table::Table, ConnectorError> {
        self.runtime
            .control_state()
            .invalidate_table_cache(namespace, table);
        self.runtime
            .load_table(namespace, table)
            .map(|loaded| loaded.into_table())
            .map_err(map_provider_error)
    }
}

impl IcebergDataMutationBackend for RegisteredIcebergDataMutationBackend {
    /// Publish the marker snapshot on the provider-private fence ref derived
    /// from this operation's stable id.
    ///
    /// The table is loaded from the fence's *own* resource identity rather than
    /// from a registered plan: establishing a fence must not require an exact
    /// prepared operation, because a recovering owner has to be able to fence an
    /// operation whose runtime state it never had. For the same reason this
    /// deliberately does not run `validate_frozen_table`, which asserts the
    /// table still matches an exact plan the recovering owner does not hold.
    fn establish_fence(
        &self,
        facts: &IcebergWriteFenceFacts,
    ) -> Result<IcebergFenceAssertion, ConnectorError> {
        let table = self.reload_table(&facts.namespace, &facts.table_name)?;
        let file_io = table.file_io().clone();
        let catalog = Arc::clone(self.runtime.catalog());
        let facts = facts.clone();
        self.runtime
            .resources()
            .catalog_runtime()
            .block_on(async move {
                crate::commit::write_fence::establish_fence(
                    catalog.as_ref(),
                    &table,
                    &file_io,
                    &facts,
                )
                .await
            })
            .map_err(|error| internal(format!("Iceberg data mutation fence runtime: {error}")))?
            .map(|established| established.assertion)
            .map_err(fence_failure_to_connector_error)
    }

    fn plan(
        &self,
        request: &ConnectorDataMutationPlanningRequest,
    ) -> Result<
        (
            PlannedIcebergMutation,
            [u8; 32],
            ConnectorDataMutationPlanSummary,
        ),
        ConnectorError,
    > {
        let table_payload = self.provider.table_payload(request.operation().table())?;
        if table_payload.metadata_table_type.is_some() {
            return Err(invalid(
                "Iceberg data mutation requires a base table handle",
            ));
        }
        let namespace = table_payload.namespace;
        let table_name = table_payload.table;
        let table = self.reload_table(&namespace, &table_name)?;
        let metadata = table.metadata();
        let table_uuid = metadata.uuid().to_string();
        let schema_id = metadata.current_schema_id();
        let default_spec_id = metadata.default_partition_spec_id();
        let metadata_version_digest = metadata_version_digest(table.metadata_location());

        match request.operation() {
            ConnectorDataMutationOperation::RegisterExistingFiles {
                source_location, ..
            } => {
                let domain = preflight_caller_managed_source_domain(
                    source_location,
                    &self.runtime.control_state().configuration().warehouse_uri,
                    self.runtime.control_state().object_store_config(),
                )
                .map_err(|error| ConnectorError::new(ConnectorErrorKind::Unsupported, error))?;
                let manifest = plan_manifest_for_table(
                    &table,
                    source_location,
                    self.runtime.control_state().object_store_config(),
                    self.runtime.resources().catalog_runtime(),
                )
                .map_err(map_provider_error)?;
                let mapping_digest = manifest
                    .canonical_name_mapping
                    .as_deref()
                    .map(|mapping| hex_encode(Sha256::digest(mapping.as_bytes())));
                let payload = IcebergDataMutationPlanPayloadV1 {
                    version: PLAN_PAYLOAD_VERSION,
                    namespace,
                    table: table_name,
                    table_uuid,
                    target_ref: "main".to_string(),
                    base_snapshot_id: metadata.current_snapshot_id(),
                    schema_id,
                    default_spec_id,
                    metadata_version_digest_hex: hex_encode(metadata_version_digest),
                    source_location: Some(source_location.to_string()),
                    name_mapping_digest_hex: mapping_digest,
                };
                let summary = ConnectorDataMutationPlanSummary::try_new(
                    u32::try_from(manifest.records.len()).map_err(|_| {
                        ConnectorError::new(
                            ConnectorErrorKind::ResourceExhausted,
                            "ADD FILES manifest count exceeds u32",
                        )
                    })?,
                    manifest.total_rows,
                    manifest.total_bytes,
                )?;
                Ok((
                    PlannedIcebergMutation::RegisterExistingFiles {
                        payload,
                        manifest: manifest.clone(),
                        domain,
                    },
                    manifest.digest,
                    summary,
                ))
            }
            ConnectorDataMutationOperation::Truncate { target_ref, .. } => {
                if target_ref.as_ref() != "main"
                    && metadata.format_version() != crate::iceberg::spec::FormatVersion::V3
                {
                    return Err(invalid(
                        "Iceberg branch TRUNCATE requires a format-v3 table",
                    ));
                }
                let base_snapshot_id = target_snapshot_id(metadata, target_ref)?;
                let payload = IcebergDataMutationPlanPayloadV1 {
                    version: PLAN_PAYLOAD_VERSION,
                    namespace,
                    table: table_name,
                    table_uuid,
                    target_ref: target_ref.to_string(),
                    base_snapshot_id,
                    schema_id,
                    default_spec_id,
                    metadata_version_digest_hex: hex_encode(metadata_version_digest),
                    source_location: None,
                    name_mapping_digest_hex: None,
                };
                let state_digest = truncate_state_digest(&payload);
                Ok((
                    PlannedIcebergMutation::Truncate { payload },
                    state_digest,
                    ConnectorDataMutationPlanSummary::default(),
                ))
            }
        }
    }

    fn execute(
        &self,
        planned: &PlannedIcebergMutation,
        marker: &IcebergDataMutationMarkerV1,
        fencing: &novarocks_spi::connector::ConnectorWriteFencing,
    ) -> Result<CommitOutcome, CommitServiceError> {
        let payload = planned.payload();
        let table = self
            .reload_table(&payload.namespace, &payload.table)
            .map_err(connector_error_as_pre_dispatch)?;
        // Derive this attempt's fence assertion from external truth first, the
        // same way the row-DML commit path does: re-observing the fence ref is
        // what lets a superseded attempt fail closed before it inspects or
        // stages anything.
        let fence_assertion = match fencing.fence() {
            Some(spi_fence) => {
                let facts = crate::commit::write_fence::fence_facts_from_spi(spi_fence);
                Some(
                    crate::commit::write_fence::derive_established_assertion(
                        table.metadata(),
                        &facts,
                    )
                    .map_err(|error| {
                        connector_error_as_pre_dispatch(ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            error.to_string(),
                        ))
                    })?,
                )
            }
            None => None,
        };
        match planned {
            PlannedIcebergMutation::RegisterExistingFiles { .. } => {
                validate_add_files_target_shape(&table, payload)
                    .map_err(connector_error_as_pre_dispatch)?;
            }
            PlannedIcebergMutation::Truncate { .. } => {
                validate_frozen_table(&table, payload, fence_assertion.is_some())
                    .map_err(connector_error_as_pre_dispatch)?;
            }
        }
        match self.lookup_marker(
            &payload.namespace,
            &payload.table,
            &payload.target_ref,
            &marker.operation_id_hex,
            &marker.identity_digest_hex,
        ) {
            Ok(MarkerLookup::Matching { snapshot_id }) => {
                return Ok(CommitOutcome {
                    new_snapshot_id: snapshot_id,
                    written_manifest_paths: Vec::new(),
                });
            }
            Ok(MarkerLookup::Conflicting) => {
                return Err(CommitServiceError::unknown(
                    "Iceberg data mutation marker conflicted before dispatch".to_string(),
                    recovery_evidence(payload, mutation_op_kind(planned)),
                ));
            }
            Ok(MarkerLookup::Missing) => {}
            Err(error) => return Err(connector_error_as_pre_dispatch(error)),
        }

        let table_ident = TableIdent::new(
            NamespaceIdent::new(payload.namespace.clone()),
            payload.table.clone(),
        );
        let op_kind = match planned {
            PlannedIcebergMutation::RegisterExistingFiles { .. } => CommitOpKind::FastAppend,
            PlannedIcebergMutation::Truncate { .. } => CommitOpKind::Truncate,
        };
        let metadata = table.metadata();
        let staging_dir = format!(
            "{}/data/_staging/data-mutation-{}",
            metadata.location(),
            marker.operation_id_hex
        );
        let collector = Arc::new(
            IcebergCommitCollector::new(
                op_kind,
                table_ident,
                payload.base_snapshot_id,
                metadata.last_sequence_number(),
                metadata.current_schema().clone(),
                metadata.default_partition_spec().clone(),
                staging_dir,
            )
            .with_table_metadata(metadata.clone()),
        );
        if let PlannedIcebergMutation::RegisterExistingFiles { manifest, .. } = planned {
            let runtime = Arc::clone(&self.runtime);
            let source_location = payload
                .source_location
                .clone()
                .expect("ADD FILES plan has source location");
            let expected_payload = payload.clone();
            let expected_manifest = manifest.clone();
            collector.set_fast_append_attempt_guard(Arc::new(move |current| {
                validate_add_files_target_shape(current, &expected_payload)
                    .map_err(|error| error.to_string())?;
                revalidate_manifest_for_table(
                    current,
                    &source_location,
                    runtime.control_state().object_store_config(),
                    &expected_manifest,
                    runtime.resources().catalog_runtime(),
                )
                .map_err(|error| format!("ADD FILES frozen manifest changed: {error}"))?;
                validate_no_duplicate_data_files(&runtime, current, &expected_manifest)
                    .map_err(|error| error.to_string())
            }));
        }
        if let PlannedIcebergMutation::RegisterExistingFiles { manifest, .. } = planned {
            for data_file in manifest
                .to_data_files()
                .map_err(|error| connector_error_as_pre_dispatch(map_provider_error(error)))?
            {
                collector.inject_written_file(
                    data_file_to_written_file(&data_file, payload.default_spec_id).map_err(
                        |error| connector_error_as_pre_dispatch(map_provider_error(error)),
                    )?,
                );
            }
        }
        let catalog = Arc::clone(self.runtime.catalog());
        ensure_hadoop_registration(&self.runtime, &table)
            .map_err(connector_error_as_pre_dispatch)?;
        let marker_value = canonical_json(marker, "Iceberg data mutation marker")
            .map_err(connector_error_as_pre_dispatch)?;
        let snapshot_properties = BTreeMap::from([(
            MARKER_PROPERTY.to_string(),
            String::from_utf8(marker_value.to_vec()).expect("canonical JSON is UTF-8"),
        )]);
        let file_io = table.file_io().clone();
        let (fs, cleanup_path_mapper) =
            build_abort_cleanup(&self.runtime).map_err(connector_error_as_pre_dispatch)?;
        let target_ref = payload.target_ref.clone();
        let outcome = self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move {
                run_iceberg_commit(RunInput {
                    collector,
                    catalog,
                    table,
                    fs,
                    file_io,
                    cleanup_path_mapper,
                    cow_update_rewrite: None,
                    selected_rewrite: None,
                    target_ref,
                    snapshot_properties,
                    atomic_partition_replacement: None,
                })
                .await
            })
            .map_err(|error| {
                CommitServiceError::invalid_input(format!("runtime failure: {error}"))
            })??;

        self.runtime
            .control_state()
            .invalidate_table_cache(&payload.namespace, &payload.table);
        let reloaded = self
            .runtime
            .load_table(&payload.namespace, &payload.table)
            .map_err(|error| {
                CommitServiceError::finalize_failed_known_committed(
                    Some(outcome.clone()),
                    format!("reload committed Iceberg data mutation: {error}"),
                    recovery_evidence(payload, op_kind),
                )
            })?;
        if let PlannedIcebergMutation::RegisterExistingFiles { manifest, .. } = planned {
            let actual_mapping = reloaded
                .table
                .metadata()
                .properties()
                .get(crate::iceberg::spec::DEFAULT_SCHEMA_NAME_MAPPING)
                .map(|mapping| crate::schema_mapping::canonical_name_mapping(mapping))
                .transpose()
                .map_err(|error| {
                    CommitServiceError::finalize_failed_known_committed(
                        Some(outcome.clone()),
                        format!("validate committed schema name mapping: {error}"),
                        recovery_evidence(payload, op_kind),
                    )
                })?;
            if actual_mapping.as_deref() != manifest.canonical_name_mapping.as_deref() {
                return Err(CommitServiceError::finalize_failed_known_committed(
                    Some(outcome),
                    "schema.name-mapping.default changed after ADD FILES commit".to_string(),
                    recovery_evidence(payload, op_kind),
                ));
            }
        }
        Ok(outcome)
    }

    fn lookup_marker(
        &self,
        namespace: &str,
        table: &str,
        target_ref: &str,
        operation_id_hex: &str,
        identity_digest_hex: &str,
    ) -> Result<MarkerLookup, ConnectorError> {
        let table = self.reload_table(namespace, table)?;
        let metadata = table.metadata();
        let target_snapshot = target_snapshot_id(metadata, target_ref)?;
        let mut by_id = HashMap::new();
        for snapshot in metadata.snapshots() {
            by_id.insert(snapshot.snapshot_id(), snapshot);
        }
        let mut cursor = target_snapshot;
        let mut visited = HashSet::new();
        while let Some(snapshot_id) = cursor {
            if !visited.insert(snapshot_id) {
                return Err(corrupt("Iceberg snapshot ancestry contains a cycle"));
            }
            let Some(snapshot) = by_id.get(&snapshot_id) else {
                break;
            };
            if let Some(raw) = snapshot
                .summary()
                .additional_properties
                .get(MARKER_PROPERTY)
            {
                let marker: IcebergDataMutationMarkerV1 =
                    decode_canonical_json(raw.as_bytes(), "Iceberg data mutation marker")?;
                if marker.operation_id_hex == operation_id_hex {
                    return Ok(if marker.identity_digest_hex == identity_digest_hex {
                        MarkerLookup::Matching { snapshot_id }
                    } else {
                        MarkerLookup::Conflicting
                    });
                }
            }
            cursor = snapshot.parent_snapshot_id();
        }
        Ok(MarkerLookup::Missing)
    }
}

pub struct IcebergDataMutationAdapter {
    key: ConnectorExecutionBindingKey,
    descriptor: ConnectorInstanceDescriptor,
    backend: Arc<dyn IcebergDataMutationBackend>,
    plans: Mutex<HashMap<ConnectorMutationOperationId, CachedPlan>>,
    terminal: Mutex<HashMap<ConnectorMutationOperationId, TerminalRecord>>,
}

impl IcebergDataMutationAdapter {
    pub(crate) fn try_new(provider: Arc<IcebergControlProvider>) -> Result<Self, ConnectorError> {
        let key = ConnectorExecutionBindingKey {
            instance_id: provider.descriptor().instance_id.clone(),
            incarnation: provider.incarnation(),
        };
        Self::new_with_backend(
            key,
            Arc::new(RegisteredIcebergDataMutationBackend::new(provider)),
        )
    }

    fn new_with_backend(
        key: ConnectorExecutionBindingKey,
        backend: Arc<dyn IcebergDataMutationBackend>,
    ) -> Result<Self, ConnectorError> {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")?,
            instance_id: key.instance_id.clone(),
        };
        Ok(Self {
            key,
            descriptor,
            backend,
            plans: Mutex::new(HashMap::new()),
            terminal: Mutex::new(HashMap::new()),
        })
    }

    fn ensure_owner(&self, owner: &ConnectorExecutionBindingKey) -> Result<(), ConnectorError> {
        if owner != &self.key {
            return Err(invalid(
                "Iceberg data mutation does not match the exact connector generation",
            ));
        }
        Ok(())
    }

    fn marker(
        &self,
        plan: &ConnectorDataMutationPlan,
        payload: &IcebergDataMutationPlanPayloadV1,
    ) -> IcebergDataMutationMarkerV1 {
        let summary = plan.summary();
        IcebergDataMutationMarkerV1 {
            version: MARKER_VALUE_VERSION,
            identity_digest_hex: hex_encode(identity_digest(&self.descriptor, &self.key, plan)),
            incarnation_hex: hex_encode(self.key.incarnation.to_bytes()),
            operation_id_hex: hex_encode(plan.operation_id().to_bytes()),
            operation_kind: plan.operation_kind().to_string(),
            request_digest_hex: hex_encode(plan.request_digest()),
            plan_digest_hex: hex_encode(plan.plan_digest()),
            state_digest_hex: hex_encode(plan.state_digest()),
            target_ref: payload.target_ref.clone(),
            base_snapshot_id: payload.base_snapshot_id,
            file_count: summary.file_count(),
            row_count: summary.row_count(),
            total_bytes: summary.total_bytes(),
        }
    }

    fn receipt(
        &self,
        plan: &ConnectorDataMutationPlan,
        snapshot_id: i64,
    ) -> Result<ConnectorDataMutationReceipt, ConnectorError> {
        ConnectorDataMutationReceipt::try_new(
            self.descriptor.clone(),
            self.key.incarnation,
            plan.operation_id(),
            plan.operation_kind(),
            plan.request_digest(),
            plan.plan_digest(),
            plan.state_digest(),
            plan.summary(),
            durable_receipt_payload(snapshot_id)?,
        )
    }

    fn evidence(
        &self,
        plan: &ConnectorDataMutationPlan,
        payload: &IcebergDataMutationPlanPayloadV1,
    ) -> Result<ExternalMutationEvidence, ConnectorError> {
        let marker = self.marker(plan, payload);
        ExternalMutationEvidence::try_new(
            EVIDENCE_PAYLOAD_VERSION,
            self.descriptor.clone(),
            self.key.incarnation,
            plan.operation_id(),
            plan.operation_kind(),
            canonical_json(
                &IcebergDataMutationEvidenceV1 {
                    version: EVIDENCE_PAYLOAD_VERSION,
                    namespace: payload.namespace.clone(),
                    table: payload.table.clone(),
                    target_ref: payload.target_ref.clone(),
                    operation_id_hex: marker.operation_id_hex,
                    operation_kind: marker.operation_kind,
                    request_digest_hex: marker.request_digest_hex,
                    plan_digest_hex: marker.plan_digest_hex,
                    state_digest_hex: marker.state_digest_hex,
                    identity_digest_hex: marker.identity_digest_hex,
                    file_count: marker.file_count,
                    row_count: marker.row_count,
                    total_bytes: marker.total_bytes,
                },
                "Iceberg data mutation evidence",
            )?,
        )
    }

    fn preflight_durable_truncate_evidence(
        &self,
        plan: &ConnectorDataMutationPlan,
        payload: &IcebergDataMutationPlanPayloadV1,
    ) -> Result<(), ConnectorError> {
        if plan.operation_kind() != TRUNCATE_OPERATION_KIND {
            return Ok(());
        }
        let wire = self.evidence(plan, payload)?.try_to_wire_v1()?;
        let hex_bytes = wire.len().checked_mul(2).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "Iceberg TRUNCATE evidence hex size overflow",
            )
        })?;
        if hex_bytes > MAX_DURABLE_TRUNCATE_EVIDENCE_HEX_BYTES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                format!(
                    "Iceberg TRUNCATE evidence wire exceeds durable {} byte cap for a {} byte lowercase-hex journal field",
                    MAX_DURABLE_ICEBERG_TRUNCATE_EVIDENCE_WIRE_BYTES,
                    MAX_DURABLE_TRUNCATE_EVIDENCE_HEX_BYTES,
                ),
            ));
        }
        Ok(())
    }

    fn committed(
        &self,
        plan: &ConnectorDataMutationPlan,
        snapshot_id: i64,
        finalization: ExternalMutationFinalization,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError> {
        Ok(ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::Applied,
            receipt: self.receipt(plan, snapshot_id)?,
            finalization,
        })
    }

    fn committed_from_reconcile(
        &self,
        request: &ConnectorDataMutationReconcileRequest,
        evidence: &IcebergDataMutationEvidenceV1,
        snapshot_id: i64,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError> {
        let summary = ConnectorDataMutationPlanSummary::try_new(
            evidence.file_count,
            evidence.row_count,
            evidence.total_bytes,
        )?;
        let receipt = ConnectorDataMutationReceipt::try_new(
            self.descriptor.clone(),
            self.key.incarnation,
            request.operation_id,
            request.operation_kind.clone(),
            request.request_digest,
            request.plan_digest,
            request.state_digest,
            summary,
            durable_receipt_payload(snapshot_id)?,
        )?;
        Ok(ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::Applied,
            receipt,
            finalization: ExternalMutationFinalization::Complete,
        })
    }
}

impl ConnectorDataMutation for IcebergDataMutationAdapter {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.key
    }

    /// Publish this attempt's fence marker so that a later direct mutation can
    /// assert it atomically, exactly as the distributed write path does.
    ///
    /// Replaying the identical fence reuses the existing marker; a lower
    /// generation, another operation's marker, or an uninterpretable marker all
    /// refuse with a typed external-fence failure.
    ///
    /// Design: ADR-0068 (docs/adr/ADR-0068-external-write-fence-as-catalog-linearization-point.md)
    fn establish_external_fence(
        &self,
        request: ConnectorExternalFenceRequest,
    ) -> Result<ConnectorExternalFenceReceipt, ConnectorError> {
        self.ensure_owner(&request.owner)?;
        request.validate(&self.key)?;
        let facts = crate::commit::write_fence::fence_facts_from_spi(&request.fence);
        let assertion = self.backend.establish_fence(&facts)?;
        ConnectorExternalFenceReceipt::try_new(
            &request.fence,
            encode_fence_receipt_payload(&assertion),
        )
    }

    fn plan_mutation(
        &self,
        request: ConnectorDataMutationPlanningRequest,
    ) -> Result<ConnectorDataMutationPlan, ConnectorError> {
        request.validate()?;
        self.ensure_owner(request.owner())?;
        let mut plans = self
            .plans
            .lock()
            .map_err(|error| internal(format!("Iceberg data mutation plan lock: {error}")))?;
        if let Some(cached) = plans.get(&request.operation_id()) {
            if cached.request_digest == request.request_digest() {
                return Ok(cached.plan.clone());
            }
            return Err(invalid(
                "Iceberg data mutation operation was replayed with a different request",
            ));
        }
        let (private, state_digest, summary) = self.backend.plan(&request)?;
        let provider_payload = canonical_json(private.payload(), "Iceberg data mutation plan")?;
        let source_scope = match &private {
            PlannedIcebergMutation::RegisterExistingFiles { manifest, .. } => {
                Some(manifest.source_scope)
            }
            PlannedIcebergMutation::Truncate { .. } => None,
        };
        let add_files_domain = match &private {
            PlannedIcebergMutation::RegisterExistingFiles { domain, .. } => Some(*domain),
            PlannedIcebergMutation::Truncate { .. } => None,
        };
        let plan = ConnectorDataMutationPlan::try_new(
            &request,
            state_digest,
            summary,
            source_scope,
            add_files_domain,
            provider_payload,
        )?;
        self.preflight_durable_truncate_evidence(&plan, private.payload())?;
        plans.insert(
            request.operation_id(),
            CachedPlan {
                request_digest: request.request_digest(),
                plan: plan.clone(),
                private,
            },
        );
        Ok(plan)
    }

    fn execute(
        &self,
        request: ConnectorDataMutationExecuteRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError> {
        request.plan.validate()?;
        self.ensure_owner(request.plan.owner())?;
        if let Some(record) = self
            .terminal
            .lock()
            .map_err(|error| internal(format!("Iceberg data mutation terminal lock: {error}")))?
            .get(&request.plan.operation_id())
            .cloned()
        {
            if record.plan_digest == request.plan.plan_digest() {
                return Ok(record.outcome);
            }
            return Err(invalid(
                "Iceberg data mutation operation was executed with a different plan",
            ));
        }
        let cached = self
            .plans
            .lock()
            .map_err(|error| internal(format!("Iceberg data mutation plan lock: {error}")))?
            .get(&request.plan.operation_id())
            .cloned()
            .ok_or_else(|| invalid("Iceberg data mutation plan is not registered"))?;
        if cached.plan.plan_digest() != request.plan.plan_digest() {
            return Err(invalid(
                "Iceberg data mutation execute request conflicts with the planned operation",
            ));
        }
        let marker = self.marker(&request.plan, cached.private.payload());
        let outcome = match self.backend.lookup_marker(
            &marker_target(&cached.private).0,
            &marker_target(&cached.private).1,
            &marker.target_ref,
            &marker.operation_id_hex,
            &marker.identity_digest_hex,
        )? {
            MarkerLookup::Matching { snapshot_id } => self.committed(
                &request.plan,
                snapshot_id,
                ExternalMutationFinalization::Complete,
            )?,
            MarkerLookup::Conflicting => ExternalMutationOutcome::CommitUnknown {
                failure: failure(
                    ConnectorMutationFailureKind::Conflict,
                    "Iceberg data mutation marker conflicts with this operation",
                ),
                evidence: self.evidence(&request.plan, cached.private.payload())?,
            },
            MarkerLookup::Missing => {
                match self
                    .backend
                    .execute(&cached.private, &marker, &request.fence)
                {
                    Ok(commit) => self.committed(
                        &request.plan,
                        commit.new_snapshot_id,
                        ExternalMutationFinalization::Complete,
                    )?,
                    Err(CommitServiceError::KnownUncommitted { message, .. })
                    | Err(CommitServiceError::InvalidInput { message }) => {
                        ExternalMutationOutcome::KnownUncommitted {
                            failure: failure(ConnectorMutationFailureKind::Conflict, message),
                        }
                    }
                    Err(CommitServiceError::Unknown { message, .. }) => {
                        ExternalMutationOutcome::CommitUnknown {
                            failure: failure(ConnectorMutationFailureKind::Unavailable, message),
                            evidence: self.evidence(&request.plan, cached.private.payload())?,
                        }
                    }
                    Err(CommitServiceError::FinalizeFailedKnownCommitted {
                        outcome,
                        finalize_error,
                        ..
                    }) => self.committed(
                        &request.plan,
                        outcome
                            .map(|outcome| outcome.new_snapshot_id)
                            .unwrap_or_default(),
                        ExternalMutationFinalization::Failed(failure(
                            ConnectorMutationFailureKind::Internal,
                            finalize_error,
                        )),
                    )?,
                }
            }
        };
        self.terminal
            .lock()
            .map_err(|error| internal(format!("Iceberg data mutation terminal lock: {error}")))?
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
        request: ConnectorDataMutationReconcileRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError> {
        self.ensure_owner(&request.owner)?;
        let evidence: IcebergDataMutationEvidenceV1 = decode_canonical_json(
            request.evidence.provider_payload(),
            "Iceberg data mutation evidence",
        )?;
        validate_evidence_request(&request, &evidence)?;
        match self.backend.lookup_marker(
            &evidence.namespace,
            &evidence.table,
            &evidence.target_ref,
            &evidence.operation_id_hex,
            &evidence.identity_digest_hex,
        )? {
            MarkerLookup::Matching { snapshot_id } => {
                self.committed_from_reconcile(&request, &evidence, snapshot_id)
            }
            MarkerLookup::Conflicting => Ok(ExternalMutationOutcome::CommitUnknown {
                failure: failure(
                    ConnectorMutationFailureKind::Conflict,
                    "Iceberg data mutation marker conflicts with reconciliation evidence",
                ),
                evidence: request.evidence,
            }),
            MarkerLookup::Missing => Ok(ExternalMutationOutcome::CommitUnknown {
                failure: failure(
                    ConnectorMutationFailureKind::Unavailable,
                    "Iceberg data mutation marker is not yet visible",
                ),
                evidence: request.evidence,
            }),
        }
    }
}

fn durable_receipt_payload(snapshot_id: i64) -> Result<Bytes, ConnectorError> {
    let payload = canonical_json(
        &IcebergDataMutationReceiptV1 {
            version: RECEIPT_PAYLOAD_VERSION,
            snapshot_id,
        },
        "Iceberg data mutation receipt",
    )?;
    if payload.len() > MAX_DURABLE_ICEBERG_TRUNCATE_RECEIPT_PROVIDER_PAYLOAD_BYTES {
        return Err(internal(format!(
            "Iceberg TRUNCATE receipt provider payload exceeds fixed {} byte durable bound",
            MAX_DURABLE_ICEBERG_TRUNCATE_RECEIPT_PROVIDER_PAYLOAD_BYTES
        )));
    }
    Ok(payload)
}

fn marker_target(planned: &PlannedIcebergMutation) -> (String, String) {
    let payload = planned.payload();
    (payload.namespace.clone(), payload.table.clone())
}

fn mutation_op_kind(planned: &PlannedIcebergMutation) -> CommitOpKind {
    match planned {
        PlannedIcebergMutation::RegisterExistingFiles { .. } => CommitOpKind::FastAppend,
        PlannedIcebergMutation::Truncate { .. } => CommitOpKind::Truncate,
    }
}

fn validate_evidence_request(
    request: &ConnectorDataMutationReconcileRequest,
    evidence: &IcebergDataMutationEvidenceV1,
) -> Result<(), ConnectorError> {
    if evidence.version != EVIDENCE_PAYLOAD_VERSION
        || evidence.operation_id_hex != hex_encode(request.operation_id.to_bytes())
        || evidence.operation_kind != request.operation_kind.as_ref()
        || evidence.request_digest_hex != hex_encode(request.request_digest)
        || evidence.plan_digest_hex != hex_encode(request.plan_digest)
        || evidence.state_digest_hex != hex_encode(request.state_digest)
    {
        return Err(invalid(
            "Iceberg data mutation evidence does not match its reconcile request",
        ));
    }
    Ok(())
}

/// Fail closed unless the table is still the base state this plan froze.
///
/// `fence_is_established` says this attempt already published its own fence
/// marker on this table. That publication is itself a metadata commit, so it
/// necessarily advances the table's metadata version; see the comment on the
/// version comparison below for why that one dimension then yields to the fence
/// assertion instead of rejecting the operation.
fn validate_frozen_table(
    table: &crate::iceberg::table::Table,
    payload: &IcebergDataMutationPlanPayloadV1,
    fence_is_established: bool,
) -> Result<(), ConnectorError> {
    let metadata = table.metadata();
    if metadata.uuid().to_string() != payload.table_uuid
        || metadata.current_schema_id() != payload.schema_id
        || metadata.default_partition_spec_id() != payload.default_spec_id
        || target_snapshot_id(metadata, &payload.target_ref)? != payload.base_snapshot_id
    {
        return Err(conflict(
            "Iceberg data mutation table state advanced after planning",
        ));
    }
    // The metadata *version* is a pre-dispatch heuristic, not something the
    // commit can assert. This attempt's own fence marker is published as an
    // ordinary metadata commit, so comparing the version after establishing a
    // fence would reject every fenced mutation for its own act. When the fence
    // is established, its assertion replaces this check with a stronger one:
    // the commit pins the fence ref to this attempt's marker atomically, so a
    // superseded owner is refused inside the catalog update rather than here.
    if !fence_is_established
        && hex_encode(metadata_version_digest(table.metadata_location()))
            != payload.metadata_version_digest_hex
    {
        return Err(conflict(
            "Iceberg data mutation table state advanced after planning",
        ));
    }
    Ok(())
}

/// ADD FILES intentionally permits a data-ref OCC refresh. Its immutable
/// contract is table identity/schema/spec plus the complete frozen manifest;
/// the attempt guard re-runs the latter on every refreshed base.
fn validate_add_files_target_shape(
    table: &crate::iceberg::table::Table,
    payload: &IcebergDataMutationPlanPayloadV1,
) -> Result<(), ConnectorError> {
    let metadata = table.metadata();
    if metadata.uuid().to_string() != payload.table_uuid
        || metadata.current_schema_id() != payload.schema_id
        || metadata.default_partition_spec_id() != payload.default_spec_id
    {
        return Err(conflict(
            "Iceberg ADD FILES target identity, schema, or partition spec changed after planning",
        ));
    }
    Ok(())
}

fn data_file_to_written_file(
    data_file: &crate::iceberg::spec::DataFile,
    partition_spec_id: i32,
) -> Result<WrittenFile, String> {
    Ok(WrittenFile {
        path: data_file.file_path().to_string(),
        format: data_file.file_format(),
        content: data_file.content_type(),
        partition_values: data_file.partition().clone(),
        partition_spec_id,
        record_count: data_file.record_count(),
        file_size_in_bytes: data_file.file_size_in_bytes(),
        split_offsets: data_file
            .split_offsets()
            .map(|offsets| offsets.to_vec())
            .unwrap_or_default(),
        column_sizes: data_file.column_sizes().clone(),
        value_counts: data_file.value_counts().clone(),
        null_value_counts: data_file.null_value_counts().clone(),
        nan_value_counts: data_file.nan_value_counts().clone(),
        lower_bounds: data_file.lower_bounds().clone(),
        upper_bounds: data_file.upper_bounds().clone(),
        key_metadata: data_file.key_metadata().map(|value| value.to_vec()),
        referenced_data_file: data_file
            .referenced_data_file()
            .map(|value| value.to_string()),
        equality_ids: data_file.equality_ids(),
        first_row_id: data_file.first_row_id(),
        content_offset: None,
        content_size_in_bytes: None,
        cardinality: None,
    })
}

fn validate_no_duplicate_data_files(
    runtime: &IcebergControlRuntime,
    table: &crate::iceberg::table::Table,
    manifest: &AddFilesManifest,
) -> Result<(), ConnectorError> {
    let table = table.clone();
    let live = runtime
        .resources()
        .catalog_runtime()
        .block_on(async move { crate::manifest::extract_data_files_with_stats(&table).await })
        .map_err(map_provider_error)?
        .map_err(map_provider_error)?
        .into_iter()
        .map(|file| file.path)
        .collect::<HashSet<_>>();
    if let Some(duplicate) = manifest
        .records
        .iter()
        .find(|record| live.contains(&record.location))
    {
        return Err(conflict(format!(
            "ADD FILES source already exists in the target table: {}",
            duplicate.location
        )));
    }
    Ok(())
}

fn build_abort_cleanup(
    runtime: &IcebergControlRuntime,
) -> Result<(crate::opendal::Operator, Option<CleanupPathMapper>), ConnectorError> {
    let state = runtime.control_state();
    let warehouse_uri = &state.configuration().warehouse_uri;
    if let Some(s3_config) = state.object_store_config() {
        let access = fs_io::resolve_access_for_location(warehouse_uri, Some(s3_config)).map_err(
            |error| {
                internal(format!(
                    "resolve Iceberg warehouse for data mutation cleanup: {error}"
                ))
            },
        )?;
        let bucket = access
            .handle()
            .authority()
            .ok_or_else(|| corrupt("Iceberg warehouse URI has no object-store bucket"))?
            .to_string();
        let mapper: CleanupPathMapper = Arc::new(move |path| {
            novarocks_fs::parse_object_store_path_parse_only(path)
                .ok()
                .and_then(|(actual_bucket, key)| (actual_bucket == bucket).then_some(key))
                .unwrap_or_else(|| path.to_string())
        });
        return Ok((access.operator(), Some(mapper)));
    }
    let fs = novarocks_fs::FsAccessResolver::new()
        .resolve_location("/__novarocks_local_root__", None)
        .map_err(|error| internal(format!("build local cleanup operator: {error}")))?
        .operator();
    let mapper: CleanupPathMapper =
        Arc::new(|path: &str| path.strip_prefix("file://").unwrap_or(path).to_string());
    Ok((fs, Some(mapper)))
}

fn ensure_hadoop_registration(
    runtime: &IcebergControlRuntime,
    table: &crate::iceberg::table::Table,
) -> Result<(), ConnectorError> {
    if runtime.control_state().uses_remote_catalog() {
        return Ok(());
    }
    let namespace = table.identifier().namespace().clone();
    let ident = table.identifier().clone();
    let metadata_location = table
        .metadata_location()
        .ok_or_else(|| corrupt("Iceberg table has no metadata location"))?
        .to_string();
    let catalog = Arc::clone(runtime.catalog());
    runtime
        .resources()
        .catalog_runtime()
        .block_on(async move {
            let _ = catalog.create_namespace(&namespace, HashMap::new()).await;
            catalog.register_table(&ident, metadata_location).await
        })
        .map_err(|error| internal(format!("Iceberg registration runtime: {error}")))?
        .map(|_| ())
        .map_err(|error| map_provider_error(error.to_string()))
}

fn target_snapshot_id(
    metadata: &crate::iceberg::spec::TableMetadata,
    target_ref: &str,
) -> Result<Option<i64>, ConnectorError> {
    if target_ref == "main" {
        return Ok(metadata
            .refs()
            .get("main")
            .map(|reference| reference.snapshot_id)
            .or_else(|| metadata.current_snapshot_id()));
    }
    metadata
        .refs()
        .get(target_ref)
        .map(|reference| Some(reference.snapshot_id))
        .ok_or_else(|| ConnectorError::new(ConnectorErrorKind::NotFound, "Iceberg ref not found"))
}

fn identity_digest(
    descriptor: &ConnectorInstanceDescriptor,
    key: &ConnectorExecutionBindingKey,
    plan: &ConnectorDataMutationPlan,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(IDENTITY_DIGEST_DOMAIN);
    digest_bytes(&mut hasher, descriptor.provider_id.as_str().as_bytes());
    digest_bytes(&mut hasher, descriptor.instance_id.as_str().as_bytes());
    digest_bytes(&mut hasher, &key.incarnation.to_bytes());
    digest_bytes(&mut hasher, &plan.operation_id().to_bytes());
    digest_bytes(&mut hasher, plan.operation_kind().as_bytes());
    digest_bytes(&mut hasher, &plan.request_digest());
    digest_bytes(&mut hasher, &plan.plan_digest());
    digest_bytes(&mut hasher, &plan.state_digest());
    hasher.finalize().into()
}

fn truncate_state_digest(payload: &IcebergDataMutationPlanPayloadV1) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(TRUNCATE_STATE_DIGEST_DOMAIN);
    digest_bytes(&mut hasher, payload.table_uuid.as_bytes());
    digest_bytes(&mut hasher, payload.target_ref.as_bytes());
    digest_bytes(
        &mut hasher,
        &payload.base_snapshot_id.unwrap_or_default().to_be_bytes(),
    );
    digest_bytes(&mut hasher, &payload.schema_id.to_be_bytes());
    digest_bytes(&mut hasher, &payload.default_spec_id.to_be_bytes());
    digest_bytes(&mut hasher, payload.metadata_version_digest_hex.as_bytes());
    hasher.finalize().into()
}

fn metadata_version_digest(metadata_location: Option<&str>) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(METADATA_VERSION_DIGEST_DOMAIN);
    digest_bytes(
        &mut hasher,
        metadata_location.unwrap_or_default().as_bytes(),
    );
    hasher.finalize().into()
}

fn digest_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(bytes);
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

fn recovery_evidence(
    payload: &IcebergDataMutationPlanPayloadV1,
    op_kind: CommitOpKind,
) -> RecoveryEvidence {
    RecoveryEvidence {
        table_ident: format!("{}.{}", payload.namespace, payload.table),
        op_kind,
        base_snapshot_id: payload.base_snapshot_id,
        base_sequence_number: 0,
        staging_dir: String::new(),
        manifest_cleanup_token: None,
    }
}

fn connector_error_as_pre_dispatch(error: ConnectorError) -> CommitServiceError {
    CommitServiceError::known_uncommitted(error.to_string(), CleanupAttempt::not_attempted())
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

fn failure(
    kind: ConnectorMutationFailureKind,
    message: impl Into<Arc<str>>,
) -> ConnectorMutationFailure {
    ConnectorMutationFailure::new(kind, message)
}

fn map_provider_error(message: impl ToString) -> ConnectorError {
    let message = message.to_string();
    let lower = message.to_ascii_lowercase();
    let kind = if lower.contains("not found") || lower.contains("unknown table") {
        ConnectorErrorKind::NotFound
    } else if lower.contains("exceed") || lower.contains("too many") {
        ConnectorErrorKind::ResourceExhausted
    } else if lower.contains("unsupported") || lower.contains("supports only") {
        ConnectorErrorKind::Unsupported
    } else if lower.contains("changed") || lower.contains("conflict") {
        ConnectorErrorKind::InvalidRequest
    } else {
        ConnectorErrorKind::Internal
    };
    ConnectorError::new(kind, message)
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

fn conflict(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}

fn internal(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorDataMutationExecuteRequest,
        ConnectorDataMutationPlanningRequest, ConnectorDataMutationReconcileRequest,
        ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorInstanceIncarnation,
        ConnectorMetadata, ConnectorProviderId, ConnectorRequestContext, ConnectorTableHandle,
        ConnectorTableIdentity, ConnectorTableRequest, ConnectorTableResolution,
    };

    use crate::access_binding::IcebergReadBinding;
    use crate::catalog_control::IcebergCatalogControlState;
    use crate::iceberg::spec::{FormatVersion, NestedField, PrimitiveType, Schema, Type};
    use crate::iceberg::{NamespaceIdent, TableCreation};
    use crate::resources::IcebergControlResources;

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    struct FakeBackend {
        lookup: Mutex<MarkerLookup>,
        execute_count: AtomicUsize,
        namespace: String,
    }

    impl FakeBackend {
        fn new() -> Self {
            Self {
                lookup: Mutex::new(MarkerLookup::Missing),
                execute_count: AtomicUsize::new(0),
                namespace: "db".to_string(),
            }
        }

        fn with_namespace(namespace: impl Into<String>) -> Self {
            Self {
                namespace: namespace.into(),
                ..Self::new()
            }
        }
    }

    impl IcebergDataMutationBackend for FakeBackend {
        /// This fake owns no catalog, so it cannot publish a marker. Refusing
        /// keeps the "a fence is a published marker" invariant true even in
        /// focused tests: nothing here may look fenced without one.
        fn establish_fence(
            &self,
            _facts: &IcebergWriteFenceFacts,
        ) -> Result<IcebergFenceAssertion, ConnectorError> {
            Err(internal(
                "fake Iceberg data mutation backend cannot publish a fence marker",
            ))
        }

        fn plan(
            &self,
            _request: &ConnectorDataMutationPlanningRequest,
        ) -> Result<
            (
                PlannedIcebergMutation,
                [u8; 32],
                ConnectorDataMutationPlanSummary,
            ),
            ConnectorError,
        > {
            Ok((
                PlannedIcebergMutation::Truncate {
                    payload: IcebergDataMutationPlanPayloadV1 {
                        version: PLAN_PAYLOAD_VERSION,
                        namespace: self.namespace.clone(),
                        table: "orders".to_string(),
                        table_uuid: "table-uuid".to_string(),
                        target_ref: "main".to_string(),
                        base_snapshot_id: Some(7),
                        schema_id: 1,
                        default_spec_id: 0,
                        metadata_version_digest_hex: "aa".repeat(32),
                        source_location: None,
                        name_mapping_digest_hex: None,
                    },
                },
                [9; 32],
                ConnectorDataMutationPlanSummary::default(),
            ))
        }

        fn execute(
            &self,
            planned: &PlannedIcebergMutation,
            _marker: &IcebergDataMutationMarkerV1,
            _fencing: &novarocks_spi::connector::ConnectorWriteFencing,
        ) -> Result<CommitOutcome, CommitServiceError> {
            self.execute_count.fetch_add(1, Ordering::SeqCst);
            Err(CommitServiceError::unknown(
                "response lost".to_string(),
                recovery_evidence(planned.payload(), CommitOpKind::Truncate),
            ))
        }

        fn lookup_marker(
            &self,
            _namespace: &str,
            _table: &str,
            _target_ref: &str,
            _operation_id_hex: &str,
            _identity_digest_hex: &str,
        ) -> Result<MarkerLookup, ConnectorError> {
            Ok(*self.lookup.lock().expect("lookup"))
        }
    }

    fn test_context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            Arc::new(NeverCancelled),
            1024,
            4096,
        )
        .expect("context")
    }

    fn table_context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            Arc::new(NeverCancelled),
            64 * 1024,
            256 * 1024,
        )
        .expect("table context")
    }

    fn exact_provider_with_empty_table() -> (
        tokio::runtime::Runtime,
        tempfile::TempDir,
        Arc<IcebergControlProvider>,
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
        let resources = IcebergControlResources::new(binding, executor.handle().clone());
        let runtime = Arc::new(
            IcebergControlRuntime::try_new(
                IcebergCatalogControlState::new(configuration),
                resources,
            )
            .expect("control runtime"),
        );
        let catalog = Arc::clone(runtime.catalog());
        executor.block_on(async move {
            let namespace = NamespaceIdent::new("db".to_string());
            catalog
                .create_namespace(&namespace, HashMap::new())
                .await
                .expect("create namespace");
            let schema = Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "value", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .expect("schema");
            catalog
                .create_table(
                    &namespace,
                    TableCreation::builder()
                        .name("t".to_string())
                        .schema(schema)
                        .format_version(FormatVersion::V2)
                        .build(),
                )
                .await
                .expect("create table");
        });
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
            instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
        };
        let provider = Arc::new(IcebergControlProvider::new(
            descriptor,
            ConnectorInstanceIncarnation::from_bytes([8; 16]),
            runtime,
        ));
        (executor, warehouse, provider)
    }

    fn test_adapter(
        backend: Arc<FakeBackend>,
    ) -> (
        IcebergDataMutationAdapter,
        ConnectorExecutionBindingKey,
        ConnectorInstanceId,
    ) {
        let instance_id = ConnectorInstanceId::parse("ice").expect("instance");
        let key = ConnectorExecutionBindingKey {
            instance_id: instance_id.clone(),
            incarnation: ConnectorInstanceIncarnation::from_bytes([3; 16]),
        };
        (
            IcebergDataMutationAdapter::new_with_backend(key.clone(), backend).expect("adapter"),
            key,
            instance_id,
        )
    }

    fn truncate_request(
        key: ConnectorExecutionBindingKey,
        instance_id: ConnectorInstanceId,
        operation_id: ConnectorMutationOperationId,
        target_ref: &str,
    ) -> ConnectorDataMutationPlanningRequest {
        let handle = ConnectorTableHandle::try_new(instance_id, Bytes::from_static(b"table"))
            .expect("handle");
        ConnectorDataMutationPlanningRequest::try_new(
            operation_id,
            key,
            ConnectorDataMutationOperation::truncate(handle, target_ref).expect("operation"),
            test_context(),
        )
        .expect("request")
    }

    #[test]
    fn exact_runtime_truncate_commits_and_replays_without_a_catalog_registry() {
        let (_executor, _warehouse, provider) = exact_provider_with_empty_table();
        let adapter = IcebergDataMutationAdapter::try_new(Arc::clone(&provider)).expect("adapter");
        let metadata = provider
            .load_table(ConnectorTableRequest {
                table: ConnectorTableIdentity {
                    instance_id: provider.descriptor().instance_id.clone(),
                    namespace: Arc::from("db"),
                    table: Arc::from("t"),
                },
                resolution: ConnectorTableResolution::StrictBaseTable,
                context: table_context(),
            })
            .expect("load table");
        let operation_id = ConnectorMutationOperationId::new();
        let planning = ConnectorDataMutationPlanningRequest::try_new(
            operation_id,
            adapter.binding_key().clone(),
            ConnectorDataMutationOperation::truncate(metadata.table, "main")
                .expect("truncate operation"),
            table_context(),
        )
        .expect("planning request");
        let plan = adapter.plan_mutation(planning).expect("plan truncate");
        let request = ConnectorDataMutationExecuteRequest::try_new(
            plan,
            novarocks_spi::connector::ConnectorWriteFencing::NotFencedByThisPhase {
                reason: "test does not exercise direct-mutation fencing",
            },
            table_context(),
        )
        .expect("execute request");
        let first = adapter.execute(request.clone()).expect("execute truncate");
        let replay = adapter.execute(request).expect("replay truncate");
        assert!(matches!(
            first,
            ExternalMutationOutcome::KnownCommitted { .. }
        ));
        assert_eq!(first, replay);
    }

    /// The SPI fence value one direct-mutation attempt seals, at an explicit
    /// generation. The frontend derives the write operation id from the
    /// direct-mutation operation id, so the fence and the mutation name the same
    /// operation and cannot borrow another statement's marker.
    fn direct_mutation_fence(
        operation_id: ConnectorMutationOperationId,
        control_plane_incarnation: u64,
        resource_epoch: u64,
        coordination_attempt: u64,
    ) -> novarocks_spi::connector::ConnectorExternalOperationFence {
        novarocks_spi::connector::ConnectorExternalOperationFence::try_new(
            novarocks_spi::connector::ConnectorClusterIdentity::derive(
                "iceberg-direct-mutation-test-cluster",
            )
            .expect("cluster identity"),
            novarocks_spi::connector::ConnectorExternalFenceGeneration::try_new(
                control_plane_incarnation,
                resource_epoch,
                coordination_attempt,
            )
            .expect("fence generation"),
            novarocks_spi::connector::ConnectorWriteOperationId::from_bytes(
                operation_id.to_bytes(),
            ),
            [8; 16],
            ConnectorTableIdentity {
                instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
                namespace: Arc::from("db"),
                table: Arc::from("t"),
            },
            novarocks_spi::connector::ConnectorWriteTargetRef::main(),
        )
        .expect("external operation fence")
    }

    fn fence_request(
        adapter: &IcebergDataMutationAdapter,
        fence: &novarocks_spi::connector::ConnectorExternalOperationFence,
    ) -> novarocks_spi::connector::ConnectorExternalFenceRequest {
        novarocks_spi::connector::ConnectorExternalFenceRequest {
            owner: adapter.binding_key().clone(),
            fence: fence.clone(),
            context: table_context(),
        }
    }

    fn reload_physical_table(provider: &IcebergControlProvider) -> crate::iceberg::table::Table {
        provider
            .runtime()
            .control_state()
            .invalidate_table_cache("db", "t");
        provider
            .runtime()
            .load_table("db", "t")
            .expect("reload table")
            .into_table()
    }

    /// The property the whole direct-mutation fence exists for: establishing it
    /// publishes a marker on external truth, so the fenced execute can derive
    /// its assertion and actually commit.
    ///
    /// A lease that only remembered the fence locally would leave `execute`
    /// deriving its assertion from a table with no marker, and every fenced
    /// TRUNCATE would fail closed with `NotEstablished`.
    #[test]
    fn a_fenced_truncate_publishes_its_marker_and_commits() {
        let (_executor, _warehouse, provider) = exact_provider_with_empty_table();
        let adapter = IcebergDataMutationAdapter::try_new(Arc::clone(&provider)).expect("adapter");
        let handle = provider
            .load_table(ConnectorTableRequest {
                table: ConnectorTableIdentity {
                    instance_id: provider.descriptor().instance_id.clone(),
                    namespace: Arc::from("db"),
                    table: Arc::from("t"),
                },
                resolution: ConnectorTableResolution::StrictBaseTable,
                context: table_context(),
            })
            .expect("load table")
            .table;
        let operation_id = ConnectorMutationOperationId::new();

        // Production order: plan first, then fence, then dispatch.
        let plan = adapter
            .plan_mutation(
                ConnectorDataMutationPlanningRequest::try_new(
                    operation_id,
                    adapter.binding_key().clone(),
                    ConnectorDataMutationOperation::truncate(handle, "main")
                        .expect("truncate operation"),
                    table_context(),
                )
                .expect("planning request"),
            )
            .expect("plan truncate");

        let fence = direct_mutation_fence(operation_id, 1, 1, 1);
        let facts = crate::commit::write_fence::fence_facts_from_spi(&fence);

        // Nothing is fenced yet, so a fenced execute has no authority at all.
        let error = crate::commit::write_fence::derive_established_assertion(
            reload_physical_table(&provider).metadata(),
            &facts,
        )
        .expect_err("an unestablished fence must not yield an assertion");
        assert!(
            matches!(
                error,
                crate::commit::write_fence::FenceError::NotEstablished { .. }
            ),
            "an unpublished marker must be NotEstablished, got {error:?}"
        );

        let receipt = adapter
            .establish_external_fence(fence_request(&adapter, &fence))
            .expect("establish the direct mutation fence");
        assert!(
            receipt.matches(&fence),
            "the receipt must acknowledge exactly this fence"
        );

        // The marker is now external truth: this is the assertion the fenced
        // execute carries into its atomic catalog update.
        crate::commit::write_fence::derive_established_assertion(
            reload_physical_table(&provider).metadata(),
            &facts,
        )
        .expect("the published marker must be derivable from external truth");

        let outcome = adapter
            .execute(
                ConnectorDataMutationExecuteRequest::try_new(
                    plan,
                    novarocks_spi::connector::ConnectorWriteFencing::Fenced(fence),
                    table_context(),
                )
                .expect("execute request"),
            )
            .expect("fenced truncate must reach the catalog");
        assert!(
            matches!(outcome, ExternalMutationOutcome::KnownCommitted { .. }),
            "a fenced TRUNCATE must actually commit, got {outcome:?}"
        );
    }

    /// A strictly higher generation of the same operation supersedes the
    /// established fence: the later owner takes the marker over, and the older
    /// attempt can no longer derive an assertion.
    #[test]
    fn a_higher_direct_mutation_fence_generation_supersedes_the_established_marker() {
        let (_executor, _warehouse, provider) = exact_provider_with_empty_table();
        let adapter = IcebergDataMutationAdapter::try_new(Arc::clone(&provider)).expect("adapter");
        let operation_id = ConnectorMutationOperationId::new();

        let first = direct_mutation_fence(operation_id, 1, 1, 1);
        let first_facts = crate::commit::write_fence::fence_facts_from_spi(&first);
        adapter
            .establish_external_fence(fence_request(&adapter, &first))
            .expect("establish the first fence");

        // Replaying the identical fence is idempotent, not a second marker.
        let replay = adapter
            .establish_external_fence(fence_request(&adapter, &first))
            .expect("replaying an identical fence must be idempotent");
        assert!(replay.matches(&first));

        let second = direct_mutation_fence(operation_id, 1, 1, 2);
        let second_facts = crate::commit::write_fence::fence_facts_from_spi(&second);
        let raised = adapter
            .establish_external_fence(fence_request(&adapter, &second))
            .expect("a strictly higher generation must supersede");
        assert!(raised.matches(&second));

        let metadata = reload_physical_table(&provider);
        crate::commit::write_fence::derive_established_assertion(
            metadata.metadata(),
            &second_facts,
        )
        .expect("the raised fence must own the marker");
        let error = crate::commit::write_fence::derive_established_assertion(
            metadata.metadata(),
            &first_facts,
        )
        .expect_err("a superseded generation must not keep its assertion");
        assert!(
            matches!(
                error,
                crate::commit::write_fence::FenceError::Superseded { .. }
            ),
            "the older attempt must be reported superseded, got {error:?}"
        );

        // And the older attempt cannot re-establish behind the raised fence.
        let error = adapter
            .establish_external_fence(fence_request(&adapter, &first))
            .expect_err("a lower generation must not be able to fence again");
        assert_eq!(
            error.external_fence_failure(),
            Some(novarocks_spi::connector::ConnectorExternalFenceFailure::Superseded),
            "a superseded fence must stay a typed fence failure, got {error:?}"
        );
    }

    #[test]
    fn marker_codec_is_canonical_and_rejects_unknown_fields() {
        let marker = IcebergDataMutationMarkerV1 {
            version: 1,
            identity_digest_hex: "11".repeat(32),
            incarnation_hex: "22".repeat(16),
            operation_id_hex: "33".repeat(16),
            operation_kind: "truncate".to_string(),
            request_digest_hex: "44".repeat(32),
            plan_digest_hex: "55".repeat(32),
            state_digest_hex: "66".repeat(32),
            target_ref: "main".to_string(),
            base_snapshot_id: Some(7),
            file_count: 0,
            row_count: 0,
            total_bytes: 0,
        };
        let encoded = canonical_json(&marker, "marker").expect("encode");
        assert_eq!(
            decode_canonical_json::<IcebergDataMutationMarkerV1>(&encoded, "marker")
                .expect("decode"),
            marker
        );
        let mut value: serde_json::Value = serde_json::from_slice(&encoded).expect("json");
        value["credential"] = serde_json::Value::String("secret".to_string());
        assert!(
            decode_canonical_json::<IcebergDataMutationMarkerV1>(
                &serde_json::to_vec(&value).expect("json"),
                "marker"
            )
            .is_err()
        );
    }

    #[test]
    fn truncate_state_digest_binds_ref_and_base() {
        let mut payload = IcebergDataMutationPlanPayloadV1 {
            version: 1,
            namespace: "db".to_string(),
            table: "orders".to_string(),
            table_uuid: "uuid".to_string(),
            target_ref: "main".to_string(),
            base_snapshot_id: Some(7),
            schema_id: 1,
            default_spec_id: 0,
            metadata_version_digest_hex: "aa".repeat(32),
            source_location: None,
            name_mapping_digest_hex: None,
        };
        let first = truncate_state_digest(&payload);
        payload.target_ref = "dev".to_string();
        assert_ne!(first, truncate_state_digest(&payload));
        payload.target_ref = "main".to_string();
        payload.base_snapshot_id = Some(8);
        assert_ne!(first, truncate_state_digest(&payload));
    }

    #[test]
    fn truncate_evidence_wire_fits_exact_durable_hex_boundary_and_rejects_one_over() {
        fn planned_evidence_wire_len(
            adapter: &IcebergDataMutationAdapter,
            plan: &ConnectorDataMutationPlan,
        ) -> usize {
            let plans = adapter.plans.lock().expect("plans");
            let cached = plans.get(&plan.operation_id()).expect("cached plan");
            adapter
                .evidence(plan, cached.private.payload())
                .expect("evidence")
                .try_to_wire_v1()
                .expect("wire")
                .len()
        }

        assert_eq!(
            MAX_DURABLE_ICEBERG_TRUNCATE_EVIDENCE_WIRE_BYTES
                .checked_mul(2)
                .expect("hex size"),
            MAX_DURABLE_TRUNCATE_EVIDENCE_HEX_BYTES
        );

        let empty_backend = Arc::new(FakeBackend::with_namespace(""));
        let (empty_adapter, key, instance_id) = test_adapter(empty_backend);
        let base_plan = empty_adapter
            .plan_mutation(truncate_request(
                key,
                instance_id,
                ConnectorMutationOperationId::from_bytes([11; 16]),
                "main",
            ))
            .expect("base plan");
        let base_wire_len = planned_evidence_wire_len(&empty_adapter, &base_plan);
        let boundary_namespace_len = MAX_DURABLE_ICEBERG_TRUNCATE_EVIDENCE_WIRE_BYTES
            .checked_sub(base_wire_len)
            .expect("evidence base must fit durable cap");

        let boundary_backend = Arc::new(FakeBackend::with_namespace(
            "n".repeat(boundary_namespace_len),
        ));
        let (boundary_adapter, key, instance_id) = test_adapter(Arc::clone(&boundary_backend));
        let boundary_plan = boundary_adapter
            .plan_mutation(truncate_request(
                key,
                instance_id,
                ConnectorMutationOperationId::from_bytes([12; 16]),
                "main",
            ))
            .expect("evidence exactly at durable cap must plan");
        assert_eq!(
            planned_evidence_wire_len(&boundary_adapter, &boundary_plan),
            MAX_DURABLE_ICEBERG_TRUNCATE_EVIDENCE_WIRE_BYTES
        );
        assert_eq!(boundary_backend.execute_count.load(Ordering::SeqCst), 0);

        let over_backend = Arc::new(FakeBackend::with_namespace(
            "n".repeat(boundary_namespace_len + 1),
        ));
        let (over_adapter, key, instance_id) = test_adapter(Arc::clone(&over_backend));
        let error = over_adapter
            .plan_mutation(truncate_request(
                key,
                instance_id,
                ConnectorMutationOperationId::from_bytes([13; 16]),
                "main",
            ))
            .expect_err("over-budget evidence must fail during planning");
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
        assert!(over_adapter.plans.lock().expect("plans").is_empty());
        assert_eq!(over_backend.execute_count.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn truncate_receipt_provider_payload_has_a_fixed_small_durable_bound() {
        for snapshot_id in [i64::MIN, -1, 0, 1, i64::MAX] {
            let payload = durable_receipt_payload(snapshot_id).expect("receipt payload");
            assert!(payload.len() <= MAX_DURABLE_ICEBERG_TRUNCATE_RECEIPT_PROVIDER_PAYLOAD_BYTES);
        }
    }

    #[test]
    fn operation_replay_is_idempotent_and_conflicting_request_is_rejected() {
        let backend = Arc::new(FakeBackend::new());
        let (adapter, key, instance_id) = test_adapter(backend);
        let operation_id = ConnectorMutationOperationId::from_bytes([8; 16]);
        let request = truncate_request(key.clone(), instance_id.clone(), operation_id, "main");
        let first = adapter.plan_mutation(request.clone()).expect("first plan");
        let replay = adapter.plan_mutation(request).expect("replay plan");
        assert_eq!(first.plan_digest(), replay.plan_digest());
        let conflict = truncate_request(key, instance_id, operation_id, "dev");
        assert!(adapter.plan_mutation(conflict).is_err());
    }

    #[test]
    fn unknown_is_not_reexecuted_and_reconcile_survives_adapter_restart() {
        let backend = Arc::new(FakeBackend::new());
        let (adapter, key, instance_id) = test_adapter(Arc::clone(&backend));
        let operation_id = ConnectorMutationOperationId::from_bytes([9; 16]);
        let plan = adapter
            .plan_mutation(truncate_request(
                key.clone(),
                instance_id,
                operation_id,
                "main",
            ))
            .expect("plan");
        let execute = ConnectorDataMutationExecuteRequest::try_new(
            plan.clone(),
            novarocks_spi::connector::ConnectorWriteFencing::NotFencedByThisPhase {
                reason: "test does not exercise direct-mutation fencing",
            },
            test_context(),
        )
        .expect("execute");
        let first = adapter.execute(execute.clone()).expect("unknown");
        let evidence = match first {
            ExternalMutationOutcome::CommitUnknown { evidence, .. } => evidence,
            other => panic!("expected unknown, got {other:?}"),
        };
        assert!(matches!(
            adapter.execute(execute).expect("cached unknown"),
            ExternalMutationOutcome::CommitUnknown { .. }
        ));
        assert_eq!(backend.execute_count.load(Ordering::SeqCst), 1);

        *backend.lookup.lock().expect("lookup") = MarkerLookup::Matching { snapshot_id: 42 };
        let restarted =
            IcebergDataMutationAdapter::new_with_backend(key, backend).expect("restart adapter");
        let reconcile =
            ConnectorDataMutationReconcileRequest::try_new(&plan, evidence, test_context())
                .expect("reconcile request");
        assert!(matches!(
            restarted.reconcile(reconcile).expect("reconciled"),
            ExternalMutationOutcome::KnownCommitted { receipt, .. }
                if receipt.summary() == ConnectorDataMutationPlanSummary::default()
        ));
    }

    #[test]
    fn reconcile_marker_matrix_is_typed_and_never_reexecutes() {
        let backend = Arc::new(FakeBackend::new());
        let (adapter, key, instance_id) = test_adapter(Arc::clone(&backend));
        let plan = adapter
            .plan_mutation(truncate_request(
                key.clone(),
                instance_id,
                ConnectorMutationOperationId::from_bytes([10; 16]),
                "main",
            ))
            .expect("plan");
        let execute = ConnectorDataMutationExecuteRequest::try_new(
            plan.clone(),
            novarocks_spi::connector::ConnectorWriteFencing::NotFencedByThisPhase {
                reason: "test does not exercise direct-mutation fencing",
            },
            test_context(),
        )
        .expect("execute");
        let ExternalMutationOutcome::CommitUnknown { evidence, .. } =
            adapter.execute(execute).expect("unknown")
        else {
            panic!("expected unknown");
        };

        let reconcile = || {
            ConnectorDataMutationReconcileRequest::try_new(&plan, evidence.clone(), test_context())
                .expect("reconcile request")
        };
        let restarted = IcebergDataMutationAdapter::new_with_backend(
            key.clone(),
            Arc::clone(&backend) as Arc<dyn IcebergDataMutationBackend>,
        )
        .expect("restart adapter");
        assert!(matches!(
            restarted.reconcile(reconcile()).expect("missing marker"),
            ExternalMutationOutcome::CommitUnknown { .. }
        ));

        *backend.lookup.lock().expect("lookup") = MarkerLookup::Conflicting;
        assert!(matches!(
            restarted
                .reconcile(reconcile())
                .expect("conflicting marker"),
            ExternalMutationOutcome::CommitUnknown { failure, .. }
                if failure.kind() == ConnectorMutationFailureKind::Conflict
        ));

        *backend.lookup.lock().expect("lookup") = MarkerLookup::Matching { snapshot_id: 43 };
        assert!(matches!(
            restarted.reconcile(reconcile()).expect("matching marker"),
            ExternalMutationOutcome::KnownCommitted { .. }
        ));
        assert_eq!(backend.execute_count.load(Ordering::SeqCst), 1);
    }
}
