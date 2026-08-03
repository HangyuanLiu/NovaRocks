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

//! Provider binding for one immutable Iceberg change-stream topology.
//!
//! The SQL planner assigns writer fragments and the application owner retains
//! the exact write lease. This module derives all provider-private terminal
//! facts from that single frozen topology, so a committer and its BE handles
//! cannot drift apart.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use bytes::Bytes;
use sha2::{Digest, Sha256};

use super::catalog::IcebergCatalogEntry;
use super::change_stream_routing::ChangeStreamWriterCommitPlan;
use super::commit::DeletionVector;
use super::sink::build_position_delete_data_file_partition_index;
use super::sink_plan::IcebergSinkObjectStoreConfig;
use super::write_commit::IcebergWriteCommitExecutor;
use super::write_contract::{
    encode_data_sink_spec_handle_payload, encode_deletion_vector_sink_handle_payload,
};
use super::write_control::IcebergWritePlanPayloadV1;
use super::write_service::{
    IcebergChangeStreamWriteReportCommitter, IcebergWriteControlService,
    IcebergWriteControlServiceContext, IcebergWriteReportCommitter,
};
use crate::engine::query_planning::bindings::QueryTableBindingStore;
use crate::engine::query_planning::write_sink::{
    IcebergWriteSinkMode, IcebergWriteSinkSpec, iceberg_write_sink_spec_from_admitted_sql_input,
};
use crate::sql::planner::distributed::write::change_stream::SqlChangeStreamWriteTopology;
use novarocks_spi::connector::{ConnectorError, ConnectorWriteOperationId};

/// Frozen application input accepted by the Iceberg provider binding. It has
/// no SQL AST, mutable topology, writer-fragment allocation, or catalog cache.
pub(crate) struct IcebergChangeStreamProviderRequest<'a> {
    pub(crate) target: &'a str,
    pub(crate) target_ref: &'a str,
    pub(crate) table: &'a iceberg::table::Table,
    pub(crate) entry: &'a IcebergCatalogEntry,
    pub(crate) base_snapshot_id: Option<i64>,
    pub(crate) operation_id: ConnectorWriteOperationId,
    pub(crate) topology: &'a SqlChangeStreamWriteTopology,
    pub(crate) table_bindings: &'a QueryTableBindingStore,
    pub(crate) commit_executor: Arc<IcebergWriteCommitExecutor>,
}

/// Opaque provider state retained between exact-lease admission and the first
/// writer plan request. Its plan, terminal handles, service factory and
/// activation digest are all derived together from one immutable topology.
pub(crate) struct IcebergChangeStreamProviderBinding {
    target_ref: String,
    provider_payload: Bytes,
    terminal_handle_payloads: BTreeMap<i32, Bytes>,
    committer: Arc<dyn IcebergWriteReportCommitter>,
    commit_plan: ChangeStreamWriterCommitPlan,
    activation_digest: [u8; 32],
}

impl IcebergChangeStreamProviderBinding {
    pub(crate) fn target_ref(&self) -> &str {
        &self.target_ref
    }

    pub(crate) fn provider_payload(&self) -> Bytes {
        self.provider_payload.clone()
    }

    pub(crate) fn activation_digest(&self) -> [u8; 32] {
        self.activation_digest
    }

    pub(crate) fn commit_plan(&self) -> &ChangeStreamWriterCommitPlan {
        &self.commit_plan
    }

    pub(crate) fn control_service(&self) -> Result<IcebergWriteControlService, ConnectorError> {
        let context = IcebergWriteControlServiceContext::new_with_fragment_handle_payloads(
            self.terminal_handle_payloads.clone(),
            IcebergWritePlanPayloadV1::decode(&self.provider_payload)?,
            Arc::clone(&self.committer),
        )?;
        Ok(IcebergWriteControlService::new(context))
    }

    pub(crate) fn control_service_factory(
        &self,
    ) -> impl Fn()
        -> Result<Arc<dyn super::write_control::IcebergWriteControlBackend>, ConnectorError>
    + Send
    + Sync
    + 'static {
        let handles = self.terminal_handle_payloads.clone();
        let payload = self.provider_payload.clone();
        let committer = Arc::clone(&self.committer);
        move || {
            let context = IcebergWriteControlServiceContext::new_with_fragment_handle_payloads(
                handles.clone(),
                IcebergWritePlanPayloadV1::decode(&payload)?,
                Arc::clone(&committer),
            )?;
            Ok(Arc::new(IcebergWriteControlService::new(context)))
        }
    }
}

/// ADR-0034: terminal handle routing, aggregate commit routing and lazy
/// activation must be derived from the same planner-frozen topology.
pub(crate) fn bind_iceberg_change_stream_provider(
    request: IcebergChangeStreamProviderRequest<'_>,
) -> Result<IcebergChangeStreamProviderBinding, String> {
    if request.commit_executor.target_ref != request.target_ref {
        return Err(
            "Iceberg change-stream provider target ref drifted from commit executor".to_string(),
        );
    }
    let commit_plan = ChangeStreamWriterCommitPlan::from_topology(request.topology)?;
    let terminal_handle_payloads = change_stream_writer_handle_payloads(
        request.topology,
        request.table,
        request.entry,
        request.base_snapshot_id,
        request.table_bindings,
    )?;
    let plan_payload = IcebergWritePlanPayloadV1 {
        version: 1,
        target: request.target.to_string(),
        target_ref: request.target_ref.to_string(),
    };
    let provider_payload = plan_payload
        .encode()
        .map_err(|error| format!("encode Iceberg change-stream plan payload: {error}"))?;
    let mut hasher = Sha256::new();
    hasher.update(request.operation_id.to_bytes());
    hasher.update(provider_payload.as_ref());
    for (fragment_id, payload) in &terminal_handle_payloads {
        hasher.update(fragment_id.to_be_bytes());
        hasher.update(payload.as_ref());
    }
    let activation_digest = hasher.finalize().into();
    let committer: Arc<dyn IcebergWriteReportCommitter> = Arc::new(
        IcebergChangeStreamWriteReportCommitter::new(request.commit_executor, commit_plan.clone()),
    );
    Ok(IcebergChangeStreamProviderBinding {
        target_ref: request.target_ref.to_string(),
        provider_payload,
        terminal_handle_payloads,
        committer,
        commit_plan,
        activation_digest,
    })
}

/// Freeze all deletion-vector facts needed by a BE-only writer at the exact
/// base snapshot. Credentials and catalog clients remain on the FE.
pub(crate) fn frozen_deletion_vector_handle_payload(
    sink_spec: &IcebergWriteSinkSpec,
    table: &iceberg::table::Table,
    entry: &IcebergCatalogEntry,
    base_snapshot_id: Option<i64>,
) -> Result<Bytes, String> {
    let metadata = table.metadata();
    let position_index_storage = position_delete_index_storage_config(entry, metadata.location())?;
    let position_delete_partitions = build_position_delete_data_file_partition_index(
        metadata,
        base_snapshot_id,
        metadata.location(),
        position_index_storage.as_ref(),
    )?;
    let existing_vectors = frozen_deletion_vectors_at_snapshot(table, base_snapshot_id, entry)?;
    encode_deletion_vector_sink_handle_payload(
        sink_spec,
        metadata,
        &position_delete_partitions,
        &existing_vectors,
    )
}

fn change_stream_writer_handle_payloads(
    topology: &SqlChangeStreamWriteTopology,
    table: &iceberg::table::Table,
    entry: &IcebergCatalogEntry,
    base_snapshot_id: Option<i64>,
    table_bindings: &QueryTableBindingStore,
) -> Result<BTreeMap<i32, Bytes>, String> {
    let mut payloads = BTreeMap::new();
    for branch in &topology.writer_branches {
        let fragment_id = i32::try_from(branch.writer_fragment_id).map_err(|_| {
            format!(
                "change-stream writer fragment {} exceeds i32 handle-map range",
                branch.writer_fragment_id
            )
        })?;
        let mut sink_spec =
            iceberg_write_sink_spec_from_admitted_sql_input(table_bindings, &branch.sink, entry)?;
        sink_spec.set_planned_snapshot_id(base_snapshot_id)?;
        let payload = match sink_spec.mode {
            IcebergWriteSinkMode::DeletionVectors => {
                frozen_deletion_vector_handle_payload(&sink_spec, table, entry, base_snapshot_id)?
            }
            IcebergWriteSinkMode::Data | IcebergWriteSinkMode::RowLineageData => {
                encode_data_sink_spec_handle_payload(&sink_spec)?
            }
            other => {
                return Err(format!(
                    "change-stream provider does not support sink mode {other:?}"
                ));
            }
        };
        if payloads.insert(fragment_id, payload).is_some() {
            return Err(format!(
                "change-stream topology has duplicate terminal writer fragment {fragment_id}"
            ));
        }
    }
    if payloads.is_empty() {
        return Err("change-stream topology has no terminal writer fragments".to_string());
    }
    Ok(payloads)
}

pub(crate) fn position_delete_index_storage_config(
    entry: &IcebergCatalogEntry,
    table_location: &str,
) -> Result<Option<IcebergSinkObjectStoreConfig>, String> {
    let Some(bucket) = super::changes::expected_object_store_bucket_from_location(table_location)?
    else {
        return Ok(None);
    };
    let config = entry.object_store_config().ok_or_else(|| {
        format!(
            "Iceberg position-delete planning requires object-store credentials for bucket {bucket}"
        )
    })?;
    Ok(Some(IcebergSinkObjectStoreConfig {
        endpoint: config.endpoint.clone(),
        bucket,
        access_key_id: config.access_key_id.clone(),
        access_key_secret: config.access_key_secret.clone(),
        session_token: config.session_token.clone(),
        region: config.region.clone(),
        enable_path_style_access: config.enable_path_style_access,
        retry_max_times: config.retry_max_times,
        retry_min_delay_ms: config.retry_min_delay_ms,
        retry_max_delay_ms: config.retry_max_delay_ms,
        timeout_ms: config.timeout_ms,
        io_timeout_ms: config.io_timeout_ms,
    }))
}

fn frozen_deletion_vectors_at_snapshot(
    table: &iceberg::table::Table,
    snapshot_id: Option<i64>,
    entry: &IcebergCatalogEntry,
) -> Result<HashMap<String, DeletionVector>, String> {
    let Some(snapshot_id) = snapshot_id else {
        return Ok(HashMap::new());
    };
    let object_store_config = entry.object_store_config();
    let factory = super::changes::build_factory_for_table(table, object_store_config)?;
    let expected_bucket = super::changes::expected_object_store_bucket_for_table(table)?;
    let positions = super::scan_deletes::previously_deleted_positions_at_snapshot(
        table,
        snapshot_id,
        &factory,
        &|path| {
            super::changes::normalize_delete_projection_path(
                path,
                object_store_config,
                expected_bucket.as_deref(),
            )
        },
        |_| true,
    )
    .map_err(|error| {
        format!("read frozen Iceberg deletion-vector positions at snapshot {snapshot_id}: {error}")
    })?;
    positions
        .into_iter()
        .map(|(path, positions)| {
            let mut vector = DeletionVector::new();
            for position in positions {
                vector.insert(position).map_err(|error| {
                    format!(
                        "encode frozen Iceberg deletion-vector position {position} for `{path}`: {error}"
                    )
                })?;
            }
            Ok((path, vector))
        })
        .collect()
}
