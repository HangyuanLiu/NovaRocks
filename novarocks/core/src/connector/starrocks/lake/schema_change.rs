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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

use arrow::array::{Array, ArrayRef, BooleanArray, UInt32Array, new_empty_array, new_null_array};
use arrow::compute::{cast, filter_record_batch, take};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use prost::Message;

use crate::connector::starrocks::lake::context::{
    PartialUpdateWritePolicy, TabletWriteContext, get_tablet_runtime, register_tablet_runtime,
    update_tablet_runtime_schema, with_txn_log_append_lock,
};
use crate::connector::starrocks::lake::schema_adapter::build_tablet_schema_from_thrift;
use crate::connector::starrocks::lake::txn_log::{
    build_tablet_output_schema, load_rowset_batch_for_partial_update_with_delete_predicates,
    parse_default_literal_to_singleton_array, read_txn_log_if_exists, write_txn_log_file,
};
use crate::connector::starrocks::ports::StorageMetadataProvider;
use crate::connector::starrocks::schema::{
    StarRocksColumnSchema, StarRocksKeysType, StarRocksTabletSchema,
};
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
use crate::exec::expr::{ExprArena, ExprId};
use crate::formats::starrocks::metadata::{
    collect_delete_predicates, lake_rowset_visibility_version,
};
use crate::formats::starrocks::writer::bundle_meta::{
    empty_tablet_metadata, load_tablet_metadata_at_version, write_initial_meta_file_with_provider,
    write_standalone_meta_file_with_provider,
};
use crate::formats::starrocks::writer::io::{read_bytes_if_exists, write_bytes};
use crate::formats::starrocks::writer::layout::{
    DATA_DIR, initial_meta_file_path, join_tablet_path, standalone_meta_file_path,
    txn_log_file_path,
};
use crate::formats::starrocks::writer::{
    StarRocksWriteFormat, build_single_segment_metadata, build_starrocks_native_segment_bytes,
    build_txn_data_file_name, sort_batch_for_native_write,
};
use crate::runtime::starlet_shard_registry::{self, S3StoreConfig};
use crate::service::grpc_client::proto::starrocks::{
    CompactionStrategyPb, PersistentIndexTypePb, RowsetMetadataPb, TabletMetadataPb, TxnLogPb,
    txn_log_pb,
};
use crate::thrift::agent_service::{
    TCompactionStrategy, TPersistentIndexType, TTabletMetaInfo, TTabletType,
    TUpdateTabletMetaInfoReq,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LakeAlterTabletMode {
    SchemaChange,
    Rollup,
}

const ALTER_METADATA_LOAD_MAX_ATTEMPTS: usize = 30;
const ALTER_METADATA_LOAD_RETRY_INTERVAL_MS: u64 = 100;

#[derive(Clone, Debug)]
pub struct RollupInputSlot {
    pub tuple_id: i32,
    pub slot_id: i32,
    pub name: String,
    pub nullable: Option<bool>,
}

#[derive(Clone, Debug)]
pub struct CompiledRollupExpression {
    pub arena: ExprArena,
    pub root: ExprId,
}

#[derive(Clone, Debug)]
pub struct RollupMaterializedViewParam {
    pub column_name: String,
    pub origin_column_name: Option<String>,
    pub expression: Option<CompiledRollupExpression>,
}

#[derive(Clone, Debug)]
pub struct RollupExpressionProgram {
    pub input_slots: Vec<RollupInputSlot>,
    pub where_expression: Option<CompiledRollupExpression>,
    pub materialized_view_params: Vec<RollupMaterializedViewParam>,
}

#[derive(Clone, Debug)]
pub struct LakeAlterTabletTask {
    pub base_tablet_id: i64,
    pub new_tablet_id: i64,
    pub base_schema_hash: i32,
    pub new_schema_hash: i32,
    pub alter_version: i64,
    pub txn_id: i64,
    pub mode: LakeAlterTabletMode,
    pub base_tablet_read_schema: Option<StarRocksTabletSchema>,
    pub rollup: Option<RollupExpressionProgram>,
    pub columns_len: usize,
    pub base_table_column_names_len: usize,
}

fn normalize_slot_name(name: &str) -> String {
    let mut s = name.trim();
    if let Some(rest) = s.strip_prefix('`').and_then(|x| x.strip_suffix('`')) {
        s = rest;
    }
    if let Some((_prefix, last)) = s.rsplit_once('.') {
        s = last;
        if let Some(rest) = s.strip_prefix('`').and_then(|x| x.strip_suffix('`')) {
            s = rest;
        }
    }
    s.to_ascii_lowercase()
}

/// Executes a lake schema-change task with the storage codec selected by the
/// composition root.  Compat must pass this explicitly: a tablet may not be
/// registered yet when the agent task arrives, so consulting the runtime
/// registry here would make the protobuf fallback observable on cold paths.
pub fn execute_lake_alter_tablet_task(
    task: LakeAlterTabletTask,
    storage_metadata_provider: Arc<dyn StorageMetadataProvider>,
) -> Result<(), String> {
    validate_schema_change_task(&task)?;

    let alter_mode = task.mode;
    let base_tablet_id = task.base_tablet_id;
    let new_tablet_id = task.new_tablet_id;
    let alter_version = task.alter_version;
    if alter_version <= 0 {
        return Err(format!(
            "alter task has invalid alter_version={alter_version}"
        ));
    }
    let txn_id = task.txn_id;
    if txn_id <= 0 {
        return Err(format!("alter task has invalid txn_id={txn_id}"));
    }

    let materialized_view_param_summary = task
        .rollup
        .as_ref()
        .map(|program| {
            program
                .materialized_view_params
                .iter()
                .map(|param| {
                    format!(
                        "{}=>origin={:?},mv_expr={}",
                        param.column_name,
                        param.origin_column_name.as_deref(),
                        param.expression.is_some()
                    )
                })
                .collect::<Vec<_>>()
                .join("; ")
        })
        .unwrap_or_default();
    let slot_desc_summary = task
        .rollup
        .as_ref()
        .map(|program| {
            program
                .input_slots
                .iter()
                .map(|slot| {
                    format!(
                        "slot_id={},col={},physical=None,parent={}",
                        slot.slot_id, slot.name, slot.tuple_id
                    )
                })
                .collect::<Vec<_>>()
                .join("; ")
        })
        .unwrap_or_default();

    tracing::info!(
        alter_mode = ?alter_mode,
        base_tablet_id,
        new_tablet_id,
        base_schema_hash = task.base_schema_hash,
        new_schema_hash = task.new_schema_hash,
        alter_version,
        txn_id,
        columns_len = task.columns_len,
        base_table_column_names_len = task.base_table_column_names_len,
        materialized_view_param_count = task.rollup.as_ref().map(|program| program.materialized_view_params.len()).unwrap_or(0),
        materialized_view_param_summary,
        slot_desc_summary,
        "schema_change alter task received"
    );
    let (base_root_path, base_s3) = resolve_tablet_location("alter_base_tablet", base_tablet_id)?;
    let (new_root_path, new_s3) = resolve_tablet_location("alter_new_tablet", new_tablet_id)?;

    let base_metadata = load_tablet_metadata_for_alter_with_retry(
        "alter_base_tablet",
        &base_root_path,
        base_tablet_id,
        alter_version,
        true,
    )?;
    let new_metadata = load_tablet_metadata_for_alter_with_retry(
        "alter_new_tablet",
        &new_root_path,
        new_tablet_id,
        1,
        true,
    )?;

    let base_read_schema = if let Some(read_schema) = task.base_tablet_read_schema.as_ref() {
        read_schema.clone()
    } else {
        resolve_tablet_schema_from_metadata_or_runtime(
            "alter_base_tablet",
            &base_metadata,
            base_tablet_id,
            alter_version,
            storage_metadata_provider.as_ref(),
        )?
    };
    let new_metadata_schema = resolve_tablet_schema_from_metadata_or_runtime(
        "alter_new_tablet",
        &new_metadata,
        new_tablet_id,
        1,
        storage_metadata_provider.as_ref(),
    )?;
    tracing::info!(
        base_schema_columns = base_read_schema.column.len(),
        new_metadata_schema_columns = new_metadata_schema.column.len(),
        "schema_change resolved base/new metadata schemas"
    );
    let new_schema = resolve_target_schema(
        &task,
        &base_read_schema,
        &new_metadata_schema,
        new_tablet_id,
    )?;
    tracing::info!(
        target_schema_columns = new_schema.column.len(),
        "schema_change resolved target schema"
    );

    if alter_mode == LakeAlterTabletMode::SchemaChange {
        ensure_schema_change_base_supported(
            &base_read_schema,
            &base_metadata,
            "base_tablet_read_schema",
        )?;
    }

    let base_storage_metadata_provider = Arc::clone(&storage_metadata_provider);
    let new_storage_metadata_provider = storage_metadata_provider;
    let base_ctx = TabletWriteContext {
        db_id: 0,
        table_id: 0,
        tablet_id: base_tablet_id,
        tablet_root_path: base_root_path,
        tablet_schema: base_read_schema.clone(),
        s3_config: base_s3,
        storage_metadata_provider: Some(base_storage_metadata_provider),
        partial_update: PartialUpdateWritePolicy::default(),
    };
    let new_ctx = TabletWriteContext {
        db_id: 0,
        table_id: 0,
        tablet_id: new_tablet_id,
        tablet_root_path: new_root_path,
        tablet_schema: new_schema.clone(),
        s3_config: new_s3,
        storage_metadata_provider: Some(new_storage_metadata_provider),
        partial_update: PartialUpdateWritePolicy::default(),
    };
    // Some rollup/sc tablets may not have been pre-registered in runtime registry (for example
    // metadata is lazily visible via shard path). Register both base/new tablet runtimes to keep
    // later publish_version lookup consistent.
    register_tablet_runtime(&base_ctx)?;
    register_tablet_runtime(&new_ctx)?;
    if should_patch_initial_metadata_schema(
        &new_metadata,
        &new_schema,
        new_ctx
            .storage_metadata_provider
            .as_deref()
            .expect("compat schema-change context installs storage metadata provider"),
    ) {
        let patched_meta = new_metadata.clone();
        let provider = new_ctx
            .storage_metadata_provider
            .as_deref()
            .expect("compat schema-change context installs storage metadata provider");
        let mut domain_metadata = provider
            .decode_tablet_metadata(&patched_meta.encode_to_vec())
            .map_err(|error| {
                format!(
                    "decode schema-change initial metadata through compat codec failed: tablet_id={} error={error}",
                    new_tablet_id
                )
            })?;
        domain_metadata.schema = Some(new_schema.clone());
        if let Some(schema_id) = new_schema.id.filter(|id| *id > 0) {
            domain_metadata
                .historical_schemas
                .entry(schema_id)
                .or_insert_with(|| new_schema.clone());
        }
        domain_metadata.id = Some(new_tablet_id);
        domain_metadata.version = Some(1);
        let initial_path = initial_meta_file_path(&new_ctx.tablet_root_path)?;
        if read_bytes_if_exists(&initial_path)?.is_some() {
            write_initial_meta_file_with_provider(
                &new_ctx.tablet_root_path,
                &domain_metadata,
                provider,
            )?;
        } else {
            let standalone_path =
                standalone_meta_file_path(&new_ctx.tablet_root_path, new_tablet_id, 1)?;
            if read_bytes_if_exists(&standalone_path)?.is_some() {
                write_standalone_meta_file_with_provider(
                    &new_ctx.tablet_root_path,
                    new_tablet_id,
                    1,
                    &domain_metadata,
                    provider,
                )?;
            } else {
                return Err(format!(
                    "schema_change could not find initial v1 metadata layout for new tablet: tablet_id={} root_path={}",
                    new_tablet_id, new_ctx.tablet_root_path
                ));
            }
        }
    }

    let source_output_schema = build_tablet_output_schema(&base_read_schema)?;
    let base_delete_predicates = collect_delete_predicates(&base_metadata)?;
    let mut rewritten_rowsets = Vec::with_capacity(base_metadata.rowsets.len());
    for (rowset_idx, source_rowset) in base_metadata.rowsets.iter().enumerate() {
        let rowset_visibility_version = lake_rowset_visibility_version(source_rowset, rowset_idx)?;
        let source_batch = load_rowset_batch_for_partial_update_with_delete_predicates(
            &base_ctx,
            source_rowset,
            rowset_visibility_version,
            &base_delete_predicates,
            &source_output_schema,
        )?;
        let transformed = transform_rowset_batch(
            &source_batch,
            &base_read_schema,
            &new_schema,
            &task,
            alter_mode,
            rowset_idx,
        )?;
        let rewritten_rowset =
            write_rewritten_rowset(&new_ctx, source_rowset, &transformed, txn_id, rowset_idx)?;
        rewritten_rowsets.push(rewritten_rowset);
    }

    write_schema_change_txn_log(
        &new_ctx.tablet_root_path,
        new_tablet_id,
        txn_id,
        alter_version,
        rewritten_rowsets,
    )
}

/// Executes a lake metadata update through the storage codec selected by the
/// composition root. Compat always supplies a codec rather than discovering
/// one from global runtime state.
pub(crate) fn execute_update_tablet_meta_info_task_with_storage_metadata_provider(
    request: &TUpdateTabletMetaInfoReq,
    storage_metadata_provider: Arc<dyn StorageMetadataProvider>,
) -> Result<(), String> {
    let tablet_type = request.tablet_type.unwrap_or(TTabletType::TABLET_TYPE_DISK);
    if tablet_type != TTabletType::TABLET_TYPE_LAKE {
        return Err(format!(
            "update_tablet_meta_info unsupported tablet_type={tablet_type:?} (only TABLET_TYPE_LAKE is supported)"
        ));
    }
    let txn_id = request
        .txn_id
        .ok_or_else(|| "update_tablet_meta_info missing txn_id".to_string())?;
    if txn_id <= 0 {
        return Err(format!(
            "update_tablet_meta_info has invalid txn_id={txn_id}"
        ));
    }
    let tablet_meta_infos = request
        .tablet_meta_infos
        .as_ref()
        .ok_or_else(|| "update_tablet_meta_info missing tablet_meta_infos".to_string())?;
    for tablet_meta_info in tablet_meta_infos {
        execute_single_tablet_meta_update(
            tablet_meta_info,
            txn_id,
            storage_metadata_provider.as_ref(),
        )?;
    }
    Ok(())
}

fn load_tablet_metadata_for_alter_with_retry(
    op: &str,
    tablet_root_path: &str,
    tablet_id: i64,
    version: i64,
    allow_missing_page_as_empty: bool,
) -> Result<TabletMetadataPb, String> {
    for attempt in 1..=ALTER_METADATA_LOAD_MAX_ATTEMPTS {
        match load_tablet_metadata_at_version(tablet_root_path, tablet_id, version) {
            Ok(Some(metadata)) => return Ok(metadata),
            Ok(None) => {
                if attempt == ALTER_METADATA_LOAD_MAX_ATTEMPTS {
                    return Err(format!(
                        "{op} metadata not found after retries: tablet_id={} version={} attempts={}",
                        tablet_id, version, ALTER_METADATA_LOAD_MAX_ATTEMPTS
                    ));
                }
                tracing::debug!(
                    op,
                    tablet_id,
                    version,
                    attempt,
                    max_attempts = ALTER_METADATA_LOAD_MAX_ATTEMPTS,
                    "alter task metadata not found, waiting for create/metadata visibility"
                );
            }
            Err(err) if is_retryable_alter_metadata_load_error(&err) => {
                if attempt == ALTER_METADATA_LOAD_MAX_ATTEMPTS {
                    if allow_missing_page_as_empty && is_missing_tablet_page_in_bundle_error(&err) {
                        tracing::warn!(
                            op,
                            tablet_id,
                            version,
                            attempts = ALTER_METADATA_LOAD_MAX_ATTEMPTS,
                            error = %err,
                            "alter task fallback to empty metadata after missing tablet page retries"
                        );
                        let mut metadata = empty_tablet_metadata(tablet_id);
                        metadata.version = Some(version);
                        return Ok(metadata);
                    }
                    return Err(format!(
                        "{op} metadata load failed after retries: tablet_id={} version={} attempts={} last_error={}",
                        tablet_id, version, ALTER_METADATA_LOAD_MAX_ATTEMPTS, err
                    ));
                }
                tracing::debug!(
                    op,
                    tablet_id,
                    version,
                    attempt,
                    max_attempts = ALTER_METADATA_LOAD_MAX_ATTEMPTS,
                    error = %err,
                    "alter task metadata is not visible yet, retrying"
                );
            }
            Err(err) => return Err(err),
        }
        sleep(Duration::from_millis(ALTER_METADATA_LOAD_RETRY_INTERVAL_MS));
    }
    Err(format!(
        "{op} exhausted metadata retry attempts unexpectedly: tablet_id={} version={} attempts={}",
        tablet_id, version, ALTER_METADATA_LOAD_MAX_ATTEMPTS
    ))
}

fn execute_single_tablet_meta_update(
    tablet_meta_info: &TTabletMetaInfo,
    txn_id: i64,
    storage_metadata_provider: &dyn StorageMetadataProvider,
) -> Result<(), String> {
    let tablet_id = tablet_meta_info
        .tablet_id
        .ok_or_else(|| "update_tablet_meta_info tablet_meta_info missing tablet_id".to_string())?;
    if tablet_id <= 0 {
        return Err(format!(
            "update_tablet_meta_info has invalid tablet_id={tablet_id}"
        ));
    }
    let (tablet_root_path, _s3) = resolve_tablet_location("update_tablet_meta_info", tablet_id)?;
    let updated_schema = tablet_meta_info
        .tablet_schema
        .as_ref()
        .map(build_tablet_schema_from_thrift)
        .transpose()?;
    write_update_tablet_meta_txn_log_with_provider(
        &tablet_root_path,
        tablet_id,
        txn_id,
        build_storage_metadata_update(tablet_meta_info)?,
        storage_metadata_provider,
    )?;
    if let Some(schema) = updated_schema.as_ref() {
        update_tablet_runtime_schema(tablet_id, schema)?;
    }
    Ok(())
}

fn build_storage_metadata_update(
    tablet_meta_info: &TTabletMetaInfo,
) -> Result<crate::connector::starrocks::lake::storage_domain::StorageMetadataUpdate, String> {
    Ok(
        crate::connector::starrocks::lake::storage_domain::StorageMetadataUpdate {
            enable_persistent_index: tablet_meta_info.enable_persistent_index,
            persistent_index_type: tablet_meta_info
                .persistent_index_type
                .map(map_update_tablet_meta_persistent_index_type)
                .transpose()?,
            bundle_tablet_metadata: tablet_meta_info.bundle_tablet_metadata,
            compaction_strategy: tablet_meta_info
                .compaction_strategy
                .map(map_update_tablet_meta_compaction_strategy)
                .transpose()?,
            flat_json_config: tablet_meta_info.flat_json_config.as_ref().map(|cfg| {
                crate::connector::starrocks::lake::storage_domain::StorageFlatJsonConfig {
                    enabled: cfg.flat_json_enable,
                    null_factor: cfg.flat_json_null_factor.map(|value| value.0),
                    sparsity_factor: cfg.flat_json_sparsity_factor.map(|value| value.0),
                    max_column_max: cfg.flat_json_column_max,
                }
            }),
            tablet_schema: tablet_meta_info
                .tablet_schema
                .as_ref()
                .map(build_tablet_schema_from_thrift)
                .transpose()?,
        },
    )
}

fn map_update_tablet_meta_persistent_index_type(
    persistent_index_type: TPersistentIndexType,
) -> Result<i32, String> {
    if persistent_index_type == TPersistentIndexType::LOCAL {
        return Ok(PersistentIndexTypePb::Local as i32);
    }
    if persistent_index_type == TPersistentIndexType::CLOUD_NATIVE {
        return Ok(PersistentIndexTypePb::CloudNative as i32);
    }
    Err(format!(
        "update_tablet_meta_info unsupported persistent_index_type={persistent_index_type:?}"
    ))
}

fn map_update_tablet_meta_compaction_strategy(
    compaction_strategy: TCompactionStrategy,
) -> Result<i32, String> {
    if compaction_strategy == TCompactionStrategy::DEFAULT {
        return Ok(CompactionStrategyPb::Default as i32);
    }
    if compaction_strategy == TCompactionStrategy::REAL_TIME {
        return Ok(CompactionStrategyPb::RealTime as i32);
    }
    Err(format!(
        "update_tablet_meta_info unsupported compaction_strategy={compaction_strategy:?}"
    ))
}

fn write_update_tablet_meta_txn_log_with_provider(
    tablet_root_path: &str,
    tablet_id: i64,
    txn_id: i64,
    metadata_update: crate::connector::starrocks::lake::storage_domain::StorageMetadataUpdate,
    storage_metadata_provider: &dyn StorageMetadataProvider,
) -> Result<(), String> {
    use crate::connector::starrocks::lake::storage_domain::{
        StorageAlterMetadataOperation, StorageTransactionLog,
    };

    let txn_log_path = txn_log_file_path(tablet_root_path, tablet_id, txn_id)?;
    with_txn_log_append_lock(tablet_id, txn_id, || {
        let mut txn_log = match read_bytes_if_exists(&txn_log_path)? {
            Some(bytes) => storage_metadata_provider.decode_transaction_log(&bytes)?,
            None => StorageTransactionLog {
                tablet_id: Some(tablet_id),
                txn_id: Some(txn_id),
                ..StorageTransactionLog::default()
            },
        };
        if txn_log.tablet_id != Some(tablet_id) {
            return Err(format!(
                "update_tablet_meta_info txn log tablet_id mismatch: expected={} actual={:?}",
                tablet_id, txn_log.tablet_id
            ));
        }
        if txn_log.txn_id != Some(txn_id) {
            return Err(format!(
                "update_tablet_meta_info txn log txn_id mismatch: expected={} actual={:?}",
                txn_id, txn_log.txn_id
            ));
        }
        if txn_log.write.is_some()
            || txn_log.compaction.is_some()
            || txn_log.schema_change.is_some()
            || txn_log.replication.is_some()
        {
            return Err(format!(
                "update_tablet_meta_info does not support mixed txn log operation: tablet_id={} txn_id={}",
                tablet_id, txn_id
            ));
        }
        txn_log
            .alter_metadata
            .get_or_insert_with(|| StorageAlterMetadataOperation {
                metadata_updates: Vec::new(),
            })
            .metadata_updates
            .push(metadata_update);
        let encoded = storage_metadata_provider.encode_transaction_log(&txn_log)?;
        write_bytes(&txn_log_path, encoded)
    })
}

fn is_retryable_alter_metadata_load_error(error: &str) -> bool {
    let lowered = error.to_ascii_lowercase();
    is_missing_tablet_page_in_bundle_error(&lowered) || lowered.contains("metadata file not found:")
}

fn is_missing_tablet_page_in_bundle_error(error: &str) -> bool {
    error.contains("bundle metadata missing tablet page for tablet_id=")
        || error.contains("bundle metadata does not contain tablet page:")
}

fn is_expected_initial_metadata_without_schema(metadata: &TabletMetadataPb, version: i64) -> bool {
    version == 1
        && metadata.schema.is_none()
        && metadata.rowsets.is_empty()
        && metadata.historical_schemas.is_empty()
}

fn should_patch_initial_metadata_schema(
    metadata: &TabletMetadataPb,
    target_schema: &StarRocksTabletSchema,
    storage_metadata_provider: &dyn StorageMetadataProvider,
) -> bool {
    storage_metadata_provider
        .decode_tablet_metadata(&metadata.encode_to_vec())
        .map(|metadata| metadata.schema.as_ref() != Some(target_schema))
        .unwrap_or(true)
}

fn resolve_tablet_schema_from_metadata_or_runtime(
    op: &str,
    metadata: &TabletMetadataPb,
    tablet_id: i64,
    version: i64,
    storage_metadata_provider: &dyn StorageMetadataProvider,
) -> Result<StarRocksTabletSchema, String> {
    if metadata.schema.is_some() {
        let domain_metadata = storage_metadata_provider
            .decode_tablet_metadata(&metadata.encode_to_vec())
            .map_err(|error| {
                format!(
                    "{op} decode tablet schema through compat codec failed: tablet_id={tablet_id} version={version} error={error}"
                )
            })?;
        return domain_metadata.schema.ok_or_else(|| {
            format!(
                "{op} compat-decoded tablet metadata missing schema: tablet_id={tablet_id} version={version}"
            )
        });
    }
    let runtime = get_tablet_runtime(tablet_id).map_err(|runtime_err| {
        format!(
            "{op} tablet metadata missing schema and runtime schema lookup failed: tablet_id={} version={} error={}",
            tablet_id, version, runtime_err
        )
    })?;
    if is_expected_initial_metadata_without_schema(metadata, version) {
        tracing::debug!(
            op,
            tablet_id,
            version,
            "alter task initial metadata does not embed schema, using runtime schema"
        );
    } else {
        tracing::warn!(
            op,
            tablet_id,
            version,
            "alter task metadata missing schema, falling back to runtime schema"
        );
    }
    Ok(runtime.schema)
}

fn validate_schema_change_task(task: &LakeAlterTabletTask) -> Result<(), String> {
    if task.base_tablet_id <= 0 {
        return Err(format!(
            "alter task has non-positive base_tablet_id={}",
            task.base_tablet_id
        ));
    }
    if task.new_tablet_id <= 0 {
        return Err(format!(
            "alter task has non-positive new_tablet_id={}",
            task.new_tablet_id
        ));
    }
    if task.alter_version <= 0 {
        return Err(format!(
            "alter task has invalid alter_version={}",
            task.alter_version
        ));
    }
    if task.txn_id <= 0 {
        return Err(format!("alter task has invalid txn_id={}", task.txn_id));
    }
    match task.mode {
        LakeAlterTabletMode::SchemaChange if task.rollup.is_some() => Err(
            "alter task does not support materialized_view_params in SCHEMA_CHANGE V1".to_string(),
        ),
        LakeAlterTabletMode::Rollup if task.rollup.is_none() => {
            Err("alter task missing desc_tbl for ROLLUP".to_string())
        }
        _ => Ok(()),
    }
}

fn resolve_tablet_location(
    op: &str,
    tablet_id: i64,
) -> Result<(String, Option<S3StoreConfig>), String> {
    match get_tablet_runtime(tablet_id) {
        Ok(runtime) => Ok((runtime.root_path, runtime.s3_config)),
        Err(runtime_err) => {
            let mut infos = starlet_shard_registry::select_infos(&[tablet_id]);
            if let Some(info) = infos.remove(&tablet_id) {
                return Ok((info.full_path, info.s3));
            }
            Err(format!(
                "{op} missing tablet runtime for tablet_id={tablet_id}: {runtime_err}"
            ))
        }
    }
}

fn ensure_schema_change_base_supported(
    schema: &StarRocksTabletSchema,
    metadata: &TabletMetadataPb,
    context: &str,
) -> Result<(), String> {
    let keys_type = schema
        .keys_type
        .ok_or_else(|| format!("{context} missing keys_type"))?;
    if keys_type != StarRocksKeysType::Primary {
        return Ok(());
    }

    let has_delvec_meta = metadata.delvec_meta.as_ref().is_some_and(|delvec_meta| {
        !delvec_meta.version_to_file.is_empty() || !delvec_meta.delvecs.is_empty()
    });
    let has_rowset_del_files = metadata
        .rowsets
        .iter()
        .any(|rowset| !rowset.del_files.is_empty());
    if has_delvec_meta || has_rowset_del_files {
        return Err(format!(
            "{context} PRIMARY_KEYS with delete vectors is unsupported for SCHEMA_CHANGE V1"
        ));
    }

    Ok(())
}

fn resolve_target_schema(
    task: &LakeAlterTabletTask,
    base_read_schema: &StarRocksTabletSchema,
    new_metadata_schema: &StarRocksTabletSchema,
    new_tablet_id: i64,
) -> Result<StarRocksTabletSchema, String> {
    if let Ok(runtime) = get_tablet_runtime(new_tablet_id) {
        if &runtime.schema != new_metadata_schema {
            tracing::info!(
                tablet_id = new_tablet_id,
                metadata_schema_id = new_metadata_schema.id,
                runtime_schema_id = runtime.schema.id,
                "schema_change target schema uses runtime schema instead of on-disk metadata"
            );
        }
        return Ok(runtime.schema);
    }
    if task.new_schema_hash != task.base_schema_hash
        && schemas_equivalent(base_read_schema, new_metadata_schema)
    {
        return Err(format!(
            "alter task target schema unresolved: new_schema_hash={} base_schema_hash={} but new tablet metadata schema is equivalent to base schema and runtime schema is unavailable",
            task.new_schema_hash, task.base_schema_hash
        ));
    }
    Ok(new_metadata_schema.clone())
}

fn schemas_equivalent(lhs: &StarRocksTabletSchema, rhs: &StarRocksTabletSchema) -> bool {
    lhs.keys_type == rhs.keys_type
        && lhs.num_short_key_columns == rhs.num_short_key_columns
        && lhs.sort_key_idxes == rhs.sort_key_idxes
        && lhs.sort_key_unique_ids == rhs.sort_key_unique_ids
        && lhs.column.len() == rhs.column.len()
        && lhs
            .column
            .iter()
            .zip(rhs.column.iter())
            .all(|(l, r)| columns_equivalent(l, r))
}

fn columns_equivalent(lhs: &StarRocksColumnSchema, rhs: &StarRocksColumnSchema) -> bool {
    lhs.unique_id == rhs.unique_id
        && lhs.name == rhs.name
        && lhs.r#type == rhs.r#type
        && lhs.is_key == rhs.is_key
        && lhs.is_nullable == rhs.is_nullable
        && lhs.aggregation == rhs.aggregation
        && lhs.default_value == rhs.default_value
        && lhs.precision == rhs.precision
        && lhs.frac == rhs.frac
        && lhs.index_length == rhs.index_length
        && lhs.children_columns.len() == rhs.children_columns.len()
        && lhs
            .children_columns
            .iter()
            .zip(rhs.children_columns.iter())
            .all(|(l, r)| columns_equivalent(l, r))
}

fn build_unique_id_index_map(
    schema: &StarRocksTabletSchema,
    context: &str,
) -> Result<HashMap<i32, usize>, String> {
    let mut out = HashMap::with_capacity(schema.column.len());
    for (idx, column) in schema.column.iter().enumerate() {
        let unique_id = column.unique_id;
        if unique_id < 0 {
            return Err(format!(
                "{context} column has negative unique_id: index={} name={} unique_id={}",
                idx,
                column.name.as_deref().unwrap_or("<unnamed>"),
                unique_id
            ));
        }
        if out.insert(unique_id, idx).is_some() {
            return Err(format!(
                "{context} duplicate unique_id detected: unique_id={}",
                unique_id
            ));
        }
    }
    Ok(out)
}

fn transform_rowset_batch(
    source_batch: &RecordBatch,
    source_schema: &StarRocksTabletSchema,
    target_schema: &StarRocksTabletSchema,
    task: &LakeAlterTabletTask,
    alter_mode: LakeAlterTabletMode,
    rowset_idx: usize,
) -> Result<RecordBatch, String> {
    match alter_mode {
        LakeAlterTabletMode::SchemaChange => transform_rowset_batch_schema_change(
            source_batch,
            source_schema,
            target_schema,
            rowset_idx,
        ),
        LakeAlterTabletMode::Rollup => transform_rowset_batch_rollup(
            source_batch,
            source_schema,
            target_schema,
            task.rollup
                .as_ref()
                .expect("validated rollup task has program"),
            rowset_idx,
        ),
    }
}

fn transform_rowset_batch_schema_change(
    source_batch: &RecordBatch,
    source_schema: &StarRocksTabletSchema,
    target_schema: &StarRocksTabletSchema,
    rowset_idx: usize,
) -> Result<RecordBatch, String> {
    let target_output_schema = build_tablet_output_schema(target_schema)?;
    if source_batch.num_rows() == 0 {
        return Ok(RecordBatch::new_empty(target_output_schema));
    }

    let source_uid_to_index =
        build_unique_id_index_map(source_schema, "schema_change source schema")?;
    let mut target_columns = Vec::with_capacity(target_schema.column.len());

    for (target_idx, target_col) in target_schema.column.iter().enumerate() {
        let target_field = target_output_schema
            .fields()
            .get(target_idx)
            .ok_or_else(|| {
                format!(
                    "schema_change target output schema index out of range: rowset_idx={} column_index={}",
                    rowset_idx, target_idx
                )
            })?;
        if target_col.unique_id < 0 {
            return Err(format!(
                "schema_change target column has negative unique_id: rowset_idx={} column_index={} name={}",
                rowset_idx,
                target_idx,
                target_col.name.as_deref().unwrap_or("<unnamed>")
            ));
        }
        let target_name = target_col.name.as_deref().unwrap_or("<unnamed>");
        if let Some(source_idx) = source_uid_to_index.get(&target_col.unique_id).copied() {
            let source_array = source_batch
                .columns()
                .get(source_idx)
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "schema_change source batch column index out of range: rowset_idx={} source_index={} source_columns={}",
                        rowset_idx,
                        source_idx,
                        source_batch.num_columns()
                    )
                })?;
            let transformed = cast_source_column_to_target(
                &source_array,
                target_field.data_type(),
                rowset_idx,
                source_idx,
                target_idx,
                target_name,
            )?;
            ensure_target_non_nullable_column(
                &transformed,
                target_col,
                rowset_idx,
                target_idx,
                target_name,
            )?;
            target_columns.push(transformed);
            continue;
        }

        let missing_column = build_missing_column_array(
            target_col,
            target_field.data_type(),
            source_batch.num_rows(),
            rowset_idx,
            target_idx,
        )?;
        ensure_target_non_nullable_column(
            &missing_column,
            target_col,
            rowset_idx,
            target_idx,
            target_name,
        )?;
        target_columns.push(missing_column);
    }

    RecordBatch::try_new(target_output_schema, target_columns).map_err(|e| {
        format!(
            "schema_change build transformed rowset batch failed: rowset_idx={} rows={} error={}",
            rowset_idx,
            source_batch.num_rows(),
            e
        )
    })
}

fn transform_rowset_batch_rollup(
    source_batch: &RecordBatch,
    source_schema: &StarRocksTabletSchema,
    target_schema: &StarRocksTabletSchema,
    program: &RollupExpressionProgram,
    rowset_idx: usize,
) -> Result<RecordBatch, String> {
    let target_output_schema = build_tablet_output_schema(target_schema)?;
    if source_batch.num_rows() == 0 {
        return Ok(RecordBatch::new_empty(target_output_schema));
    }

    let materialized_param_map = build_rollup_materialized_param_map(program)?;
    let filtered_source_batch =
        apply_rollup_where_expr(source_batch, source_schema, program, rowset_idx)?;
    if filtered_source_batch.num_rows() == 0 {
        return Ok(RecordBatch::new_empty(target_output_schema));
    }

    let source_uid_to_index = build_unique_id_index_map(source_schema, "rollup source schema")?;
    let source_name_to_index = build_source_name_index_map(source_schema, "rollup source schema")?;
    let need_mv_expr_eval = materialized_param_map
        .values()
        .any(|param| param.expression.is_some());
    let eval_input = if need_mv_expr_eval {
        Some(build_rollup_expr_input(
            program,
            &filtered_source_batch,
            source_schema,
            rowset_idx,
        )?)
    } else {
        None
    };

    let mut target_columns = Vec::with_capacity(target_schema.column.len());
    for (target_idx, target_col) in target_schema.column.iter().enumerate() {
        let target_field = target_output_schema
            .fields()
            .get(target_idx)
            .ok_or_else(|| {
                format!(
                    "rollup target output schema index out of range: rowset_idx={} column_index={}",
                    rowset_idx, target_idx
                )
            })?;
        let target_name = target_col.name.as_deref().unwrap_or("<unnamed>");
        let target_name_key = normalize_slot_name(target_name);

        let output_array = if let Some(mv_param) =
            materialized_param_map.get(&target_name_key).copied()
        {
            if let Some(mv_expr) = mv_param.expression.as_ref() {
                let eval_input = eval_input.as_ref().ok_or_else(|| {
                    format!(
                        "rollup mv_expr evaluation context is missing: rowset_idx={} target_index={} target_name={}",
                        rowset_idx, target_idx, target_name
                    )
                })?;
                let expr_array = eval_rollup_expr(
                    mv_expr,
                    eval_input,
                    "materialized_view_params.mv_expr",
                    rowset_idx,
                    target_idx,
                    target_name,
                )?;
                if expr_array.len() != filtered_source_batch.num_rows() {
                    return Err(format!(
                        "rollup mv_expr result row count mismatch: rowset_idx={} target_index={} target_name={} expected_rows={} actual_rows={}",
                        rowset_idx,
                        target_idx,
                        target_name,
                        filtered_source_batch.num_rows(),
                        expr_array.len()
                    ));
                }
                cast_rollup_expr_to_target(
                    &expr_array,
                    target_field.data_type(),
                    rowset_idx,
                    target_idx,
                    target_name,
                )?
            } else {
                let origin_name = mv_param
                    .origin_column_name
                    .as_deref()
                    .filter(|v| !v.trim().is_empty())
                    .ok_or_else(|| {
                        format!(
                            "rollup materialized_view_param missing origin_column_name without mv_expr: rowset_idx={} target_index={} target_name={}",
                            rowset_idx, target_idx, target_name
                        )
                    })?;
                let source_idx =
                    resolve_source_column_index_by_name(&source_name_to_index, origin_name).ok_or_else(
                        || {
                            format!(
                                "rollup origin column not found in source schema: rowset_idx={} target_index={} target_name={} origin_column_name={}",
                                rowset_idx, target_idx, target_name, origin_name
                            )
                        },
                    )?;
                let source_array = filtered_source_batch
                    .columns()
                    .get(source_idx)
                    .cloned()
                    .ok_or_else(|| {
                        format!(
                            "rollup source batch column index out of range for origin column: rowset_idx={} source_index={} source_columns={}",
                            rowset_idx,
                            source_idx,
                            filtered_source_batch.num_columns()
                        )
                    })?;
                cast_source_column_to_target(
                    &source_array,
                    target_field.data_type(),
                    rowset_idx,
                    source_idx,
                    target_idx,
                    target_name,
                )?
            }
        } else if let Some(source_idx) =
            resolve_source_column_index_by_name(&source_name_to_index, target_name)
                .or_else(|| source_uid_to_index.get(&target_col.unique_id).copied())
        {
            let source_array = filtered_source_batch
                .columns()
                .get(source_idx)
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "rollup source batch column index out of range: rowset_idx={} source_index={} source_columns={}",
                        rowset_idx,
                        source_idx,
                        filtered_source_batch.num_columns()
                    )
                })?;
            cast_source_column_to_target(
                &source_array,
                target_field.data_type(),
                rowset_idx,
                source_idx,
                target_idx,
                target_name,
            )?
        } else {
            build_missing_column_array(
                target_col,
                target_field.data_type(),
                filtered_source_batch.num_rows(),
                rowset_idx,
                target_idx,
            )?
        };

        ensure_target_non_nullable_column(
            &output_array,
            target_col,
            rowset_idx,
            target_idx,
            target_name,
        )?;
        target_columns.push(output_array);
    }

    RecordBatch::try_new(target_output_schema, target_columns).map_err(|e| {
        format!(
            "rollup build transformed rowset batch failed: rowset_idx={} rows={} error={}",
            rowset_idx,
            filtered_source_batch.num_rows(),
            e
        )
    })
}

fn build_source_name_index_map(
    schema: &StarRocksTabletSchema,
    context: &str,
) -> Result<HashMap<String, usize>, String> {
    let mut out = HashMap::with_capacity(schema.column.len());
    for (idx, column) in schema.column.iter().enumerate() {
        let Some(name) = column.name.as_deref().filter(|v| !v.trim().is_empty()) else {
            continue;
        };
        let normalized = normalize_slot_name(name);
        if out.insert(normalized.clone(), idx).is_some() {
            return Err(format!(
                "{context} duplicate column name detected after normalization: name={} normalized={}",
                name, normalized
            ));
        }
    }
    Ok(out)
}

fn resolve_source_column_index_by_name(
    source_name_to_index: &HashMap<String, usize>,
    source_name: &str,
) -> Option<usize> {
    source_name_to_index
        .get(&normalize_slot_name(source_name))
        .copied()
}

fn build_rollup_materialized_param_map(
    program: &RollupExpressionProgram,
) -> Result<HashMap<String, &RollupMaterializedViewParam>, String> {
    let mut out = HashMap::new();
    for (idx, param) in program.materialized_view_params.iter().enumerate() {
        let name = param.column_name.trim();
        if name.is_empty() {
            return Err(format!(
                "rollup materialized_view_params[{}] has empty column_name",
                idx
            ));
        }
        let key = normalize_slot_name(name);
        if out.insert(key.clone(), param).is_some() {
            return Err(format!(
                "rollup materialized_view_params duplicate column_name after normalization: column_name={} normalized={}",
                name, key
            ));
        }
    }
    Ok(out)
}

struct RollupExprInput {
    chunk: Chunk,
}

fn build_rollup_expr_input(
    program: &RollupExpressionProgram,
    source_batch: &RecordBatch,
    source_schema: &StarRocksTabletSchema,
    rowset_idx: usize,
) -> Result<RollupExprInput, String> {
    let source_name_to_index = build_source_name_index_map(source_schema, "rollup source schema")?;

    let mut fields = Vec::new();
    let mut slot_schemas = Vec::new();
    let mut arrays = Vec::new();
    let mut seen_slots = HashSet::new();

    for slot in &program.input_slots {
        let Some(source_idx) =
            resolve_source_column_index_by_name(&source_name_to_index, &slot.name)
        else {
            continue;
        };
        let slot_id = crate::common::ids::SlotId::try_from(slot.slot_id).map_err(|e| {
            format!(
                "rollup descriptor slot id conversion failed: rowset_idx={} slot_id={} error={}",
                rowset_idx, slot.slot_id, e
            )
        })?;
        if !seen_slots.insert(slot_id) {
            return Err(format!(
                "rollup descriptor contains duplicate slot id: rowset_idx={} slot_id={}",
                rowset_idx, slot.slot_id
            ));
        }
        let source_batch_schema = source_batch.schema();
        let source_field = source_batch_schema
            .fields()
            .get(source_idx)
            .ok_or_else(|| {
                format!(
                    "rollup source schema index out of range for expression slot mapping: rowset_idx={} source_index={} source_columns={}",
                    rowset_idx,
                    source_idx,
                    source_batch.num_columns()
                )
            })?;
        let field = Field::new(
            &slot.name,
            source_field.data_type().clone(),
            slot.nullable.unwrap_or(source_field.is_nullable()),
        );
        slot_schemas.push(ChunkSlotSchema::from_field(slot_id, &field, None)?);
        fields.push(field);
        arrays.push(
            source_batch
                .columns()
                .get(source_idx)
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "rollup source batch index out of range while building expression input: rowset_idx={} source_index={} source_columns={}",
                        rowset_idx,
                        source_idx,
                        source_batch.num_columns()
                    )
                })?,
        );
    }

    if fields.is_empty() {
        return Err(format!(
            "rollup cannot map descriptor slots to source schema for expression evaluation: rowset_idx={}",
            rowset_idx
        ));
    }

    let eval_schema = Arc::new(Schema::new(fields));
    let eval_batch = RecordBatch::try_new(eval_schema, arrays).map_err(|e| {
        format!(
            "rollup failed to build expression input batch: rowset_idx={} error={}",
            rowset_idx, e
        )
    })?;
    let chunk =
        Chunk::try_new_with_chunk_schema(eval_batch, Arc::new(ChunkSchema::try_new(slot_schemas)?))
            .map_err(|e| {
                format!(
                    "rollup failed to initialize expression input chunk: rowset_idx={} error={}",
                    rowset_idx, e
                )
            })?;
    Ok(RollupExprInput { chunk })
}

fn eval_rollup_expr(
    expr: &CompiledRollupExpression,
    eval_input: &RollupExprInput,
    expr_context: &str,
    rowset_idx: usize,
    target_idx: usize,
    target_name: &str,
) -> Result<ArrayRef, String> {
    expr.arena.eval(expr.root, &eval_input.chunk).map_err(|e| {
        format!(
            "rollup evaluate expression failed: rowset_idx={} target_index={} target_name={} context={} error={}",
            rowset_idx, target_idx, target_name, expr_context, e
        )
    })
}

fn apply_rollup_where_expr(
    source_batch: &RecordBatch,
    source_schema: &StarRocksTabletSchema,
    program: &RollupExpressionProgram,
    rowset_idx: usize,
) -> Result<RecordBatch, String> {
    let Some(where_expr) = program.where_expression.as_ref() else {
        return Ok(source_batch.clone());
    };
    let eval_input = build_rollup_expr_input(program, source_batch, source_schema, rowset_idx)?;
    let predicate = eval_rollup_expr(
        where_expr,
        &eval_input,
        "where_expr",
        rowset_idx,
        usize::MAX,
        "<where_expr>",
    )?;
    apply_rollup_where_predicate(source_batch, &predicate, rowset_idx)
}

fn apply_rollup_where_predicate(
    source_batch: &RecordBatch,
    predicate: &ArrayRef,
    rowset_idx: usize,
) -> Result<RecordBatch, String> {
    let predicate_bool = if predicate.data_type() == &DataType::Boolean {
        predicate.clone()
    } else {
        cast(predicate.as_ref(), &DataType::Boolean).map_err(|e| {
            format!(
                "rollup cast where_expr result to boolean failed: rowset_idx={} from={:?} error={}",
                rowset_idx,
                predicate.data_type(),
                e
            )
        })?
    };
    let predicate_bool = predicate_bool
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            format!(
                "rollup where_expr did not produce boolean result after cast: rowset_idx={} result_type={:?}",
                rowset_idx,
                predicate_bool.data_type()
            )
        })?;
    if predicate_bool.len() != source_batch.num_rows() {
        return Err(format!(
            "rollup where_expr row count mismatch: rowset_idx={} expected_rows={} actual_rows={}",
            rowset_idx,
            source_batch.num_rows(),
            predicate_bool.len()
        ));
    }
    if predicate_bool.is_empty() {
        return Ok(source_batch.clone());
    }

    let keep = (0..predicate_bool.len())
        .map(|row| !predicate_bool.is_null(row) && predicate_bool.value(row))
        .collect::<Vec<_>>();
    if keep.iter().all(|v| *v) {
        return Ok(source_batch.clone());
    }
    if keep.iter().all(|v| !*v) {
        return Ok(RecordBatch::new_empty(source_batch.schema()));
    }
    let mask = BooleanArray::from(keep);
    filter_record_batch(source_batch, &mask).map_err(|e| {
        format!(
            "rollup apply where_expr filter failed: rowset_idx={} rows={} error={}",
            rowset_idx,
            source_batch.num_rows(),
            e
        )
    })
}

fn cast_rollup_expr_to_target(
    expr_array: &ArrayRef,
    target_type: &DataType,
    rowset_idx: usize,
    target_idx: usize,
    target_name: &str,
) -> Result<ArrayRef, String> {
    if expr_array.data_type() == target_type {
        return Ok(expr_array.clone());
    }
    cast(expr_array.as_ref(), target_type).map_err(|e| {
        format!(
            "rollup cast mv_expr result failed: rowset_idx={} target_index={} target_name={} from={:?} to={:?} error={}",
            rowset_idx,
            target_idx,
            target_name,
            expr_array.data_type(),
            target_type,
            e
        )
    })
}

fn ensure_target_non_nullable_column(
    array: &ArrayRef,
    target_col: &StarRocksColumnSchema,
    rowset_idx: usize,
    target_idx: usize,
    target_name: &str,
) -> Result<(), String> {
    if target_col.is_nullable.unwrap_or(true) {
        return Ok(());
    }
    if array.null_count() > 0 {
        return Err(format!(
            "schema_change produced null values for non-nullable target column: rowset_idx={} target_index={} target_name={} null_count={}",
            rowset_idx,
            target_idx,
            target_name,
            array.null_count()
        ));
    }
    Ok(())
}

fn cast_source_column_to_target(
    source_array: &ArrayRef,
    target_type: &DataType,
    rowset_idx: usize,
    source_idx: usize,
    target_idx: usize,
    target_name: &str,
) -> Result<ArrayRef, String> {
    if source_array.data_type() == target_type {
        return Ok(source_array.clone());
    }
    let casted = cast(source_array.as_ref(), target_type).map_err(|e| {
        format!(
            "schema_change cast column failed: rowset_idx={} source_index={} target_index={} target_name={} from={:?} to={:?} error={}",
            rowset_idx,
            source_idx,
            target_idx,
            target_name,
            source_array.data_type(),
            target_type,
            e
        )
    })?;
    for row_idx in 0..source_array.len() {
        if !source_array.is_null(row_idx) && casted.is_null(row_idx) {
            return Err(format!(
                "schema_change cast produced null for non-null source value: rowset_idx={} source_index={} target_index={} target_name={} row_idx={} from={:?} to={:?}",
                rowset_idx,
                source_idx,
                target_idx,
                target_name,
                row_idx,
                source_array.data_type(),
                target_type
            ));
        }
    }
    Ok(casted)
}

fn build_missing_column_array(
    target_col: &StarRocksColumnSchema,
    target_type: &DataType,
    row_count: usize,
    rowset_idx: usize,
    target_idx: usize,
) -> Result<ArrayRef, String> {
    if row_count == 0 {
        return Ok(new_empty_array(target_type));
    }
    if let Some(raw_default) = target_col.default_value.as_ref() {
        let literal = String::from_utf8_lossy(raw_default).to_string();
        let singleton = parse_default_literal_to_singleton_array(target_type, &literal).map_err(|e| {
            format!(
                "schema_change parse default literal failed: rowset_idx={} target_index={} target_name={} literal={} error={}",
                rowset_idx,
                target_idx,
                target_col.name.as_deref().unwrap_or("<unnamed>"),
                literal,
                e
            )
        })?;
        return repeat_singleton_array(&singleton, row_count, rowset_idx, target_idx);
    }
    if target_col.is_nullable.unwrap_or(true) {
        return Ok(new_null_array(target_type, row_count));
    }
    Err(format!(
        "schema_change missing default value for non-nullable added column: rowset_idx={} target_index={} target_name={}",
        rowset_idx,
        target_idx,
        target_col.name.as_deref().unwrap_or("<unnamed>")
    ))
}

fn repeat_singleton_array(
    singleton: &ArrayRef,
    row_count: usize,
    rowset_idx: usize,
    target_idx: usize,
) -> Result<ArrayRef, String> {
    if singleton.len() != 1 {
        return Err(format!(
            "schema_change singleton default array length mismatch: rowset_idx={} target_index={} len={}",
            rowset_idx,
            target_idx,
            singleton.len()
        ));
    }
    let index = UInt32Array::from(vec![0_u32; row_count]);
    take(singleton.as_ref(), &index, None).map_err(|e| {
        format!(
            "schema_change repeat singleton default array failed: rowset_idx={} target_index={} rows={} error={}",
            rowset_idx, target_idx, row_count, e
        )
    })
}

fn write_rewritten_rowset(
    new_ctx: &TabletWriteContext,
    source_rowset: &RowsetMetadataPb,
    transformed_batch: &RecordBatch,
    txn_id: i64,
    rowset_idx: usize,
) -> Result<RowsetMetadataPb, String> {
    if transformed_batch.num_rows() == 0 {
        return Ok(RowsetMetadataPb {
            id: None,
            overlapped: source_rowset.overlapped.or(Some(false)),
            segments: Vec::new(),
            num_rows: Some(0),
            data_size: Some(0),
            // Rewritten rowsets materialize post-delete visible rows, so they must not
            // carry over source delete predicates or delete counters again.
            delete_predicate: None,
            num_dels: Some(0),
            segment_size: Vec::new(),
            max_compact_input_rowset_id: source_rowset.max_compact_input_rowset_id,
            version: None,
            del_files: Vec::new(),
            segment_encryption_metas: Vec::new(),
            next_compaction_offset: source_rowset.next_compaction_offset,
            bundle_file_offsets: Vec::new(),
            shared_segments: Vec::new(),
            record_predicate: source_rowset.record_predicate.clone(),
            segment_metas: Vec::new(),
        });
    }

    let sorted_batch = sort_batch_for_native_write(transformed_batch, &new_ctx.tablet_schema)?;
    let segment_meta = build_single_segment_metadata(&sorted_batch, &new_ctx.tablet_schema)?;
    let segment_bytes =
        build_starrocks_native_segment_bytes(&sorted_batch, &new_ctx.tablet_schema)?;
    let segment_size = segment_bytes.len() as u64;
    let driver_id = i32::try_from(rowset_idx).map_err(|_| {
        format!(
            "schema_change rowset index overflow while generating data file name: rowset_idx={}",
            rowset_idx
        )
    })?;
    let data_file_name = build_txn_data_file_name(
        new_ctx.tablet_id,
        txn_id,
        driver_id,
        0,
        StarRocksWriteFormat::Native,
        None,
    )?;
    let data_file_path = join_tablet_path(
        &new_ctx.tablet_root_path,
        &format!("{DATA_DIR}/{data_file_name}"),
    )?;
    write_bytes(&data_file_path, segment_bytes)?;

    Ok(RowsetMetadataPb {
        id: None,
        overlapped: source_rowset.overlapped.or(Some(false)),
        segments: vec![data_file_name],
        num_rows: Some(sorted_batch.num_rows() as i64),
        data_size: Some(segment_size as i64),
        // Delete predicates are applied while reading source rowsets for rewrite.
        delete_predicate: None,
        num_dels: Some(0),
        segment_size: vec![segment_size],
        max_compact_input_rowset_id: source_rowset.max_compact_input_rowset_id,
        version: None,
        del_files: Vec::new(),
        segment_encryption_metas: Vec::new(),
        next_compaction_offset: source_rowset.next_compaction_offset,
        bundle_file_offsets: Vec::new(),
        shared_segments: vec![false],
        record_predicate: source_rowset.record_predicate.clone(),
        segment_metas: vec![segment_meta],
    })
}

fn write_schema_change_txn_log(
    tablet_root_path: &str,
    new_tablet_id: i64,
    txn_id: i64,
    alter_version: i64,
    rewritten_rowsets: Vec<RowsetMetadataPb>,
) -> Result<(), String> {
    let txn_log_path = txn_log_file_path(tablet_root_path, new_tablet_id, txn_id)?;
    with_txn_log_append_lock(new_tablet_id, txn_id, || {
        let mut txn_log = match read_txn_log_if_exists(&txn_log_path)? {
            Some(existing) => existing,
            None => TxnLogPb {
                tablet_id: Some(new_tablet_id),
                txn_id: Some(txn_id),
                op_write: None,
                op_compaction: None,
                op_schema_change: None,
                op_alter_metadata: None,
                op_replication: None,
                partition_id: None,
                load_id: None,
            },
        };
        if txn_log.tablet_id != Some(new_tablet_id) {
            return Err(format!(
                "alter task txn log tablet_id mismatch: expected={} actual={:?}",
                new_tablet_id, txn_log.tablet_id
            ));
        }
        if txn_log.txn_id != Some(txn_id) {
            return Err(format!(
                "alter task txn log txn_id mismatch: expected={} actual={:?}",
                txn_id, txn_log.txn_id
            ));
        }
        if txn_log.op_write.is_some()
            || txn_log.op_compaction.is_some()
            || txn_log.op_alter_metadata.is_some()
            || txn_log.op_replication.is_some()
        {
            return Err(format!(
                "alter task does not support mixed txn log operation: tablet_id={} txn_id={}",
                new_tablet_id, txn_id
            ));
        }
        txn_log.op_schema_change = Some(txn_log_pb::OpSchemaChange {
            rowsets: rewritten_rowsets,
            linked_segment: Some(false),
            alter_version: Some(alter_version),
            delvec_meta: None,
        });
        write_txn_log_file(&txn_log_path, &txn_log)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_change_domain_task_rejects_rollup_program() {
        let task = LakeAlterTabletTask {
            base_tablet_id: 1,
            new_tablet_id: 2,
            base_schema_hash: 1,
            new_schema_hash: 2,
            alter_version: 1,
            txn_id: 1,
            mode: LakeAlterTabletMode::SchemaChange,
            base_tablet_read_schema: None,
            rollup: Some(RollupExpressionProgram {
                input_slots: vec![],
                where_expression: None,
                materialized_view_params: vec![],
            }),
            columns_len: 0,
            base_table_column_names_len: 0,
        };

        assert_eq!(
            validate_schema_change_task(&task).expect_err("schema-change must reject rollup"),
            "alter task does not support materialized_view_params in SCHEMA_CHANGE V1"
        );
    }

    #[test]
    fn metadata_update_domain_facts_preserve_optional_task_values() {
        let request = TTabletMetaInfo {
            enable_persistent_index: Some(true),
            persistent_index_type: Some(TPersistentIndexType::CLOUD_NATIVE),
            bundle_tablet_metadata: Some(true),
            compaction_strategy: Some(TCompactionStrategy::REAL_TIME),
            ..TTabletMetaInfo::default()
        };

        let update = build_storage_metadata_update(&request).expect("domain update facts");

        assert_eq!(update.enable_persistent_index, Some(true));
        assert_eq!(
            update.persistent_index_type,
            Some(PersistentIndexTypePb::CloudNative as i32)
        );
        assert_eq!(update.bundle_tablet_metadata, Some(true));
        assert_eq!(
            update.compaction_strategy,
            Some(CompactionStrategyPb::RealTime as i32)
        );
        assert!(update.tablet_schema.is_none());
    }
}
