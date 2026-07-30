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

use crate::connector::starrocks::lake::storage_schema_wire::{
    decode_tablet_schema, encode_tablet_schema,
};
use crate::connector::starrocks::lake::txn_log::{
    ensure_rowset_segment_meta_consistency, normalize_rowset_shared_segments,
};
use crate::connector::starrocks::schema::{StarRocksKeysType, StarRocksTabletSchema};
use crate::formats::starrocks::writer::bundle_meta::next_rowset_id;
use crate::runtime::starlet_shard_registry::S3StoreConfig;
use crate::service::grpc_client::proto::starrocks::{TabletMetadataPb, TxnLogPb, txn_log_pb};

enum TxnLogDispatch<'a> {
    Write(&'a txn_log_pb::OpWrite),
    Compaction(&'a txn_log_pb::OpCompaction),
    SchemaChange(&'a txn_log_pb::OpSchemaChange),
    AlterMetadata(&'a txn_log_pb::OpAlterMetadata),
    Unsupported(&'static str),
    Empty,
}

fn dispatch_txn_log(txn_log: &TxnLogPb) -> TxnLogDispatch<'_> {
    if let Some(op_write) = txn_log.op_write.as_ref() {
        return TxnLogDispatch::Write(op_write);
    }
    if let Some(op_compaction) = txn_log.op_compaction.as_ref() {
        return TxnLogDispatch::Compaction(op_compaction);
    }
    if let Some(op_schema_change) = txn_log.op_schema_change.as_ref() {
        return TxnLogDispatch::SchemaChange(op_schema_change);
    }
    if let Some(op_alter_metadata) = txn_log.op_alter_metadata.as_ref() {
        return TxnLogDispatch::AlterMetadata(op_alter_metadata);
    }
    if txn_log.op_replication.is_some() {
        return TxnLogDispatch::Unsupported("op_replication");
    }
    TxnLogDispatch::Empty
}

pub(crate) fn apply_txn_log_to_metadata(
    metadata: &mut TabletMetadataPb,
    txn_log: &TxnLogPb,
    default_schema_id: i64,
    tablet_schema: &StarRocksTabletSchema,
    tablet_root_path: &str,
    s3_config: Option<&S3StoreConfig>,
    apply_version: i64,
) -> Result<(), String> {
    match dispatch_txn_log(txn_log) {
        TxnLogDispatch::Write(op_write) => {
            let schema_id = maybe_update_metadata_schema_for_write(
                metadata,
                op_write,
                default_schema_id,
                tablet_schema,
                txn_log.tablet_id,
                txn_log.txn_id,
            )?;
            let txn_id = txn_log.txn_id.ok_or_else(|| {
                format!(
                    "txn log missing txn_id for publish_version: tablet_id={:?}",
                    txn_log.tablet_id
                )
            })?;
            if apply_version <= 0 {
                return Err(format!(
                    "invalid apply_version for publish_version: {}",
                    apply_version
                ));
            }

            if is_primary_keys_table(tablet_schema)? {
                let _ = (tablet_root_path, s3_config, apply_version, txn_id);
                return Err(format!(
                    "PRIMARY_KEYS publish requires the compat storage metadata provider: tablet_id={:?} txn_id={:?}",
                    txn_log.tablet_id, txn_log.txn_id
                ));
            }

            if let Some(rowset) = op_write.rowset.as_ref()
                && (rowset.num_rows.unwrap_or(0) > 0 || rowset.delete_predicate.is_some())
            {
                let mut new_rowset = rowset.clone();
                ensure_rowset_segment_meta_consistency(&new_rowset)?;
                normalize_rowset_shared_segments(&mut new_rowset);
                let rowset_id = metadata
                    .next_rowset_id
                    .unwrap_or_else(|| next_rowset_id(&metadata.rowsets));
                new_rowset.id = Some(rowset_id);
                metadata.next_rowset_id.replace(
                    rowset_id.saturating_add(std::cmp::max(1, new_rowset.segments.len()) as u32),
                );
                if !metadata.rowset_to_schema.is_empty() {
                    metadata.rowset_to_schema.insert(rowset_id, schema_id);
                }
                metadata.rowsets.push(new_rowset);
            }
            Ok(())
        }
        TxnLogDispatch::Compaction(op_compaction) => {
            if is_primary_keys_table(tablet_schema)? {
                return Err(format!(
                    "publish_version does not support op_compaction for PRIMARY_KEYS yet: tablet_id={:?} txn_id={:?}",
                    txn_log.tablet_id, txn_log.txn_id
                ));
            }
            apply_compaction_log(metadata, op_compaction, default_schema_id)
        }
        TxnLogDispatch::SchemaChange(op_schema_change) => {
            if primary_key_schema_change_requires_delvec_support(metadata, tablet_schema)? {
                return Err(format!(
                    "publish_version does not support op_schema_change for PRIMARY_KEYS with delete vectors yet: tablet_id={:?} txn_id={:?}",
                    txn_log.tablet_id, txn_log.txn_id
                ));
            }
            apply_schema_change_log(metadata, op_schema_change, default_schema_id)
        }
        TxnLogDispatch::AlterMetadata(op_alter_metadata) => {
            apply_alter_metadata_log(metadata, op_alter_metadata, txn_log)
        }
        TxnLogDispatch::Unsupported(op_name) => Err(format!(
            "publish_version does not support txn log operation {} yet: tablet_id={:?} txn_id={:?}",
            op_name, txn_log.tablet_id, txn_log.txn_id
        )),
        TxnLogDispatch::Empty => Ok(()),
    }
}

fn apply_compaction_log(
    metadata: &mut TabletMetadataPb,
    op_compaction: &txn_log_pb::OpCompaction,
    default_schema_id: i64,
) -> Result<(), String> {
    if op_compaction.input_rowsets.is_empty() {
        return Ok(());
    }

    let first_input_id = op_compaction.input_rowsets[0];
    let Some(first_input_pos) = metadata
        .rowsets
        .iter()
        .position(|rowset| rowset.id == Some(first_input_id))
    else {
        return Err(format!(
            "op_compaction input rowset {} not exist",
            first_input_id
        ));
    };

    let mut last_input_pos = first_input_pos;
    for input_rowset_id in op_compaction.input_rowsets.iter().skip(1) {
        let next_pos = last_input_pos.saturating_add(1);
        let Some(found) = metadata.rowsets.get(next_pos) else {
            return Err(format!(
                "op_compaction input rowset {} not exist",
                input_rowset_id
            ));
        };
        if found.id != Some(*input_rowset_id) {
            return Err("op_compaction input rowset position not adjacent".to_string());
        }
        last_input_pos = next_pos;
    }

    let old_input_ids = op_compaction.input_rowsets.clone();
    let current_schema_id = metadata
        .schema
        .as_ref()
        .and_then(|schema| schema.id)
        .filter(|v| *v > 0)
        .unwrap_or(default_schema_id);
    let rowset_id_base = metadata
        .next_rowset_id
        .unwrap_or_else(|| next_rowset_id(&metadata.rowsets));

    let mut output_rowset_id = None;
    let mut keep_output_rowset = false;
    if let Some(output_rowset) = op_compaction.output_rowset.as_ref()
        && output_rowset.num_rows.unwrap_or(0) > 0
    {
        let mut new_rowset = output_rowset.clone();
        ensure_rowset_segment_meta_consistency(&new_rowset)?;
        normalize_rowset_shared_segments(&mut new_rowset);
        new_rowset.id = Some(rowset_id_base);
        new_rowset.max_compact_input_rowset_id = old_input_ids.iter().copied().max();
        output_rowset_id = new_rowset.id;
        metadata.next_rowset_id =
            Some(rowset_id_base.saturating_add(std::cmp::max(1, new_rowset.segments.len()) as u32));
        metadata.rowsets[first_input_pos] = new_rowset;
        keep_output_rowset = true;
    }

    let remove_start = if keep_output_rowset {
        first_input_pos.saturating_add(1)
    } else {
        first_input_pos
    };
    metadata.rowsets.drain(remove_start..=last_input_pos);

    if !metadata.rowset_to_schema.is_empty() {
        for input_rowset_id in &old_input_ids {
            metadata.rowset_to_schema.remove(input_rowset_id);
        }
        if let Some(output_rowset_id) = output_rowset_id {
            metadata
                .rowset_to_schema
                .insert(output_rowset_id, current_schema_id);
        }

        let active_schema_ids = metadata
            .rowset_to_schema
            .values()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        metadata
            .historical_schemas
            .retain(|schema_id, _| active_schema_ids.contains(schema_id));
    }

    let mut new_cumulative_point = 0_u32;
    let current_cumulative_point = metadata.cumulative_point.unwrap_or(0);
    if first_input_pos as u32 >= current_cumulative_point {
        new_cumulative_point = first_input_pos as u32;
    } else if current_cumulative_point >= old_input_ids.len() as u32 {
        new_cumulative_point = current_cumulative_point - old_input_ids.len() as u32;
    }
    if keep_output_rowset {
        new_cumulative_point = new_cumulative_point.saturating_add(1);
    }
    if new_cumulative_point > metadata.rowsets.len() as u32 {
        return Err(format!(
            "op_compaction new cumulative point exceeds rowset size: cumulative_point={} rowsets={}",
            new_cumulative_point,
            metadata.rowsets.len()
        ));
    }
    metadata.cumulative_point = Some(new_cumulative_point);
    Ok(())
}

fn maybe_update_metadata_schema_for_write(
    metadata: &mut TabletMetadataPb,
    op_write: &txn_log_pb::OpWrite,
    default_schema_id: i64,
    runtime_schema: &StarRocksTabletSchema,
    tablet_id: Option<i64>,
    txn_id: Option<i64>,
) -> Result<i64, String> {
    let schema_key = op_write.schema_key.as_ref().ok_or_else(|| {
        format!(
            "op_write missing schema_key for publish_version: tablet_id={tablet_id:?} txn_id={txn_id:?}"
        )
    })?;
    let schema_id = schema_key
        .schema_id
        .filter(|v| *v > 0)
        .unwrap_or(default_schema_id);
    if schema_id <= 0 {
        return Err(format!(
            "op_write has non-positive schema_id after fallback: tablet_id={tablet_id:?} txn_id={txn_id:?} schema_id={schema_id} default_schema_id={default_schema_id}"
        ));
    }

    let current_schema_id = metadata
        .schema
        .as_ref()
        .and_then(|schema| schema.id)
        .unwrap_or(0);
    if current_schema_id == schema_id || metadata.historical_schemas.contains_key(&schema_id) {
        return Ok(schema_id);
    }

    let resolved_schema = if runtime_schema.id == Some(schema_id) {
        Some(runtime_schema.clone())
    } else {
        metadata
            .historical_schemas
            .get(&schema_id)
            .cloned()
            .map(decode_tablet_schema)
            .transpose()?
    };
    let resolved_schema = resolved_schema.ok_or_else(|| {
        format!(
            "publish_version cannot resolve schema from schema_key: tablet_id={tablet_id:?} txn_id={txn_id:?} schema_id={schema_id} runtime_schema_id={:?} historical_schema_ids={:?}",
            runtime_schema.id,
            metadata.historical_schemas.keys().collect::<Vec<_>>()
        )
    })?;

    apply_tablet_schema_update(
        metadata,
        &resolved_schema,
        format!("op_write schema update: tablet_id={tablet_id:?} txn_id={txn_id:?}"),
    )?;
    Ok(schema_id)
}

fn apply_tablet_schema_update(
    metadata: &mut TabletMetadataPb,
    new_schema: &StarRocksTabletSchema,
    reason: String,
) -> Result<(), String> {
    let new_schema_id = new_schema
        .id
        .ok_or_else(|| format!("tablet schema update missing schema id: reason={reason}"))?;
    if new_schema_id <= 0 {
        return Err(format!(
            "tablet schema update has non-positive schema id: reason={reason} schema_id={new_schema_id}"
        ));
    }

    if let Some(existing_schema) = metadata.schema.as_ref() {
        let existing_schema_id = existing_schema.id.unwrap_or(0);
        if existing_schema_id == new_schema_id {
            metadata
                .historical_schemas
                .entry(new_schema_id)
                .or_insert_with(|| existing_schema.clone());
            return Ok(());
        }

        if existing_schema_id > 0 {
            if metadata.rowset_to_schema.is_empty() && !metadata.rowsets.is_empty() {
                let existing_rowset_ids = metadata
                    .rowsets
                    .iter()
                    .map(|rowset| {
                        rowset.id.ok_or_else(|| {
                            format!(
                                "tablet rowset id is missing when switching schema: reason={reason} existing_schema_id={existing_schema_id}"
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?;
                for rowset_id in existing_rowset_ids {
                    metadata
                        .rowset_to_schema
                        .entry(rowset_id)
                        .or_insert(existing_schema_id);
                }
            }
            metadata
                .historical_schemas
                .entry(existing_schema_id)
                .or_insert_with(|| existing_schema.clone());
        }
    } else if !metadata.rowsets.is_empty() {
        // Recover from malformed metadata produced by historical schema-change/rollup paths:
        // rowsets exist but schema is absent. Bind existing rowsets to the incoming schema id.
        let existing_rowset_ids = metadata
            .rowsets
            .iter()
            .map(|rowset| {
                rowset.id.ok_or_else(|| {
                    format!(
                        "tablet rowset id is missing when recovering schema-less metadata: reason={reason}"
                    )
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        for rowset_id in existing_rowset_ids {
            metadata
                .rowset_to_schema
                .entry(rowset_id)
                .or_insert(new_schema_id);
        }
    }

    let encoded_schema = encode_tablet_schema(new_schema);
    metadata
        .historical_schemas
        .entry(new_schema_id)
        .or_insert_with(|| encoded_schema.clone());
    metadata.schema = Some(encoded_schema);
    Ok(())
}

fn apply_alter_metadata_log(
    metadata: &mut TabletMetadataPb,
    op_alter_metadata: &txn_log_pb::OpAlterMetadata,
    txn_log: &TxnLogPb,
) -> Result<(), String> {
    for update_info in &op_alter_metadata.metadata_update_infos {
        if let Some(enable_persistent_index) = update_info.enable_persistent_index {
            metadata.enable_persistent_index = Some(enable_persistent_index);
        }
        if let Some(persistent_index_type) = update_info.persistent_index_type {
            metadata.persistent_index_type = Some(persistent_index_type);
        }
        if let Some(compaction_strategy) = update_info.compaction_strategy {
            metadata.compaction_strategy = Some(compaction_strategy);
        }
        if let Some(flat_json_config) = update_info.flat_json_config.as_ref() {
            metadata.flat_json_config = Some(*flat_json_config);
        }
        if let Some(tablet_schema) = update_info.tablet_schema.as_ref() {
            let tablet_schema = decode_tablet_schema(tablet_schema.clone())?;
            apply_tablet_schema_update(
                metadata,
                &tablet_schema,
                format!(
                    "op_alter_metadata schema update: tablet_id={:?} txn_id={:?}",
                    txn_log.tablet_id, txn_log.txn_id
                ),
            )?;
        }
    }
    Ok(())
}

fn apply_schema_change_log(
    metadata: &mut TabletMetadataPb,
    op_schema_change: &txn_log_pb::OpSchemaChange,
    default_schema_id: i64,
) -> Result<(), String> {
    for rowset in &op_schema_change.rowsets {
        let mut new_rowset = rowset.clone();
        ensure_rowset_segment_meta_consistency(&new_rowset)?;
        normalize_rowset_shared_segments(&mut new_rowset);
        let rowset_id = metadata
            .next_rowset_id
            .unwrap_or_else(|| next_rowset_id(&metadata.rowsets));
        new_rowset.id = Some(rowset_id);
        metadata
            .next_rowset_id
            .replace(rowset_id.saturating_add(std::cmp::max(1, new_rowset.segments.len()) as u32));
        if !metadata.rowset_to_schema.is_empty() {
            metadata
                .rowset_to_schema
                .insert(rowset_id, default_schema_id);
        }
        metadata.rowsets.push(new_rowset);
    }
    if let Some(delvec_meta) = op_schema_change.delvec_meta.as_ref() {
        metadata.delvec_meta = Some(delvec_meta.clone());
    }
    Ok(())
}

fn is_primary_keys_table(tablet_schema: &StarRocksTabletSchema) -> Result<bool, String> {
    let keys_type = tablet_schema
        .keys_type
        .ok_or_else(|| "tablet schema missing keys_type for publish_version".to_string())?;
    Ok(keys_type == StarRocksKeysType::Primary)
}

fn primary_key_schema_change_requires_delvec_support(
    metadata: &TabletMetadataPb,
    tablet_schema: &StarRocksTabletSchema,
) -> Result<bool, String> {
    if !is_primary_keys_table(tablet_schema)? {
        return Ok(false);
    }
    let has_delvec_meta = metadata.delvec_meta.as_ref().is_some_and(|delvec_meta| {
        !delvec_meta.version_to_file.is_empty() || !delvec_meta.delvecs.is_empty()
    });
    let has_rowset_del_files = metadata
        .rowsets
        .iter()
        .any(|rowset| !rowset.del_files.is_empty());
    Ok(has_delvec_meta || has_rowset_del_files)
}

/// Applies the publish state machine without generated StarRocks messages.
/// Compat decodes transaction and tablet metadata before this boundary and
/// encodes the result only after the kernel has finished. Primary-key/delete-
/// vector writes use the same domain boundary while their file I/O and bitmap
/// handling remain in the core kernel.
pub(crate) fn apply_storage_txn_log_to_metadata(
    metadata: &mut crate::connector::starrocks::lake::storage_domain::StorageTabletMetadata,
    txn_log: &crate::connector::starrocks::lake::storage_domain::StorageTransactionLog,
    default_schema_id: i64,
    tablet_schema: &StarRocksTabletSchema,
    tablet_root_path: &str,
    s3_config: Option<&S3StoreConfig>,
    apply_version: i64,
) -> Result<(), String> {
    if let Some(write) = txn_log.write.as_ref() {
        let schema_id = maybe_update_storage_schema_for_write(
            metadata,
            write,
            default_schema_id,
            tablet_schema,
            txn_log.tablet_id,
            txn_log.txn_id,
        )?;
        if txn_log.txn_id.is_none() {
            return Err(format!(
                "txn log missing txn_id for publish_version: tablet_id={:?}",
                txn_log.tablet_id
            ));
        }
        if apply_version <= 0 {
            return Err(format!(
                "invalid apply_version for publish_version: {apply_version}"
            ));
        }
        if is_primary_keys_table(tablet_schema)? {
            return super::pk_applier::apply_primary_key_write_log_to_metadata(
                metadata,
                write,
                schema_id,
                tablet_schema,
                tablet_root_path,
                s3_config,
                apply_version,
                txn_log.txn_id.expect("checked above"),
            );
        }
        if let Some(rowset) = write.rowset.as_ref()
            && (rowset.num_rows.unwrap_or(0) > 0 || rowset.delete_predicate.is_some())
        {
            let mut rowset = rowset.clone();
            normalize_storage_rowset(&mut rowset)?;
            let rowset_id = metadata
                .next_rowset_id
                .unwrap_or_else(|| next_storage_rowset_id(&metadata.rowsets));
            rowset.id = Some(rowset_id);
            metadata.next_rowset_id =
                Some(rowset_id.saturating_add(std::cmp::max(1, rowset.segments.len()) as u32));
            if !metadata.rowset_to_schema.is_empty() {
                metadata.rowset_to_schema.insert(rowset_id, schema_id);
            }
            metadata.rowsets.push(rowset);
        }
        return Ok(());
    }
    if let Some(compaction) = txn_log.compaction.as_ref() {
        if is_primary_keys_table(tablet_schema)? {
            return Err(format!(
                "publish_version does not support op_compaction for PRIMARY_KEYS yet: tablet_id={:?} txn_id={:?}",
                txn_log.tablet_id, txn_log.txn_id
            ));
        }
        return apply_storage_compaction_log(metadata, compaction, default_schema_id);
    }
    if let Some(schema_change) = txn_log.schema_change.as_ref() {
        if storage_primary_key_schema_change_requires_delvec_support(metadata, tablet_schema)? {
            return Err(format!(
                "publish_version does not support op_schema_change for PRIMARY_KEYS with delete vectors yet: tablet_id={:?} txn_id={:?}",
                txn_log.tablet_id, txn_log.txn_id
            ));
        }
        return apply_storage_schema_change_log(metadata, schema_change, default_schema_id);
    }
    if let Some(alter_metadata) = txn_log.alter_metadata.as_ref() {
        return apply_storage_alter_metadata_log(metadata, alter_metadata, txn_log);
    }
    if txn_log.replication.is_some() {
        return Err(format!(
            "publish_version does not support txn log operation op_replication yet: tablet_id={:?} txn_id={:?}",
            txn_log.tablet_id, txn_log.txn_id
        ));
    }
    Ok(())
}

fn apply_storage_compaction_log(
    metadata: &mut crate::connector::starrocks::lake::storage_domain::StorageTabletMetadata,
    compaction: &crate::connector::starrocks::lake::storage_domain::StorageCompactionOperation,
    default_schema_id: i64,
) -> Result<(), String> {
    if compaction.input_rowsets.is_empty() {
        return Ok(());
    }
    let first_input_id = compaction.input_rowsets[0];
    let Some(first_input_pos) = metadata
        .rowsets
        .iter()
        .position(|rowset| rowset.id == Some(first_input_id))
    else {
        return Err(format!(
            "op_compaction input rowset {first_input_id} not exist"
        ));
    };
    let mut last_input_pos = first_input_pos;
    for input_rowset_id in compaction.input_rowsets.iter().skip(1) {
        let next_pos = last_input_pos.saturating_add(1);
        let Some(found) = metadata.rowsets.get(next_pos) else {
            return Err(format!(
                "op_compaction input rowset {input_rowset_id} not exist"
            ));
        };
        if found.id != Some(*input_rowset_id) {
            return Err("op_compaction input rowset position not adjacent".to_string());
        }
        last_input_pos = next_pos;
    }

    let old_input_ids = compaction.input_rowsets.clone();
    let current_schema_id = metadata
        .schema
        .as_ref()
        .and_then(|schema| schema.id)
        .filter(|value| *value > 0)
        .unwrap_or(default_schema_id);
    let rowset_id_base = metadata
        .next_rowset_id
        .unwrap_or_else(|| next_storage_rowset_id(&metadata.rowsets));
    let mut output_rowset_id = None;
    let mut keep_output_rowset = false;
    if let Some(output_rowset) = compaction.output_rowset.as_ref()
        && output_rowset.num_rows.unwrap_or(0) > 0
    {
        let mut rowset = output_rowset.clone();
        normalize_storage_rowset(&mut rowset)?;
        rowset.id = Some(rowset_id_base);
        rowset.max_compact_input_rowset_id = old_input_ids.iter().copied().max();
        output_rowset_id = rowset.id;
        metadata.next_rowset_id =
            Some(rowset_id_base.saturating_add(std::cmp::max(1, rowset.segments.len()) as u32));
        metadata.rowsets[first_input_pos] = rowset;
        keep_output_rowset = true;
    }
    metadata.rowsets.drain(
        if keep_output_rowset {
            first_input_pos.saturating_add(1)
        } else {
            first_input_pos
        }..=last_input_pos,
    );
    if !metadata.rowset_to_schema.is_empty() {
        for input_rowset_id in &old_input_ids {
            metadata.rowset_to_schema.remove(input_rowset_id);
        }
        if let Some(output_rowset_id) = output_rowset_id {
            metadata
                .rowset_to_schema
                .insert(output_rowset_id, current_schema_id);
        }
        let active_schema_ids = metadata
            .rowset_to_schema
            .values()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        metadata
            .historical_schemas
            .retain(|schema_id, _| active_schema_ids.contains(schema_id));
    }
    let current_cumulative_point = metadata.cumulative_point.unwrap_or(0);
    let mut cumulative_point = if first_input_pos as u32 >= current_cumulative_point {
        first_input_pos as u32
    } else if current_cumulative_point >= old_input_ids.len() as u32 {
        current_cumulative_point - old_input_ids.len() as u32
    } else {
        0
    };
    if keep_output_rowset {
        cumulative_point = cumulative_point.saturating_add(1);
    }
    if cumulative_point > metadata.rowsets.len() as u32 {
        return Err(format!(
            "op_compaction new cumulative point exceeds rowset size: cumulative_point={} rowsets={}",
            cumulative_point,
            metadata.rowsets.len()
        ));
    }
    metadata.cumulative_point = Some(cumulative_point);
    Ok(())
}

fn apply_storage_schema_change_log(
    metadata: &mut crate::connector::starrocks::lake::storage_domain::StorageTabletMetadata,
    schema_change: &crate::connector::starrocks::lake::storage_domain::StorageSchemaChangeOperation,
    default_schema_id: i64,
) -> Result<(), String> {
    for rowset in &schema_change.rowsets {
        let mut rowset = rowset.clone();
        normalize_storage_rowset(&mut rowset)?;
        let rowset_id = metadata
            .next_rowset_id
            .unwrap_or_else(|| next_storage_rowset_id(&metadata.rowsets));
        rowset.id = Some(rowset_id);
        metadata.next_rowset_id =
            Some(rowset_id.saturating_add(std::cmp::max(1, rowset.segments.len()) as u32));
        if !metadata.rowset_to_schema.is_empty() {
            metadata
                .rowset_to_schema
                .insert(rowset_id, default_schema_id);
        }
        metadata.rowsets.push(rowset);
    }
    if let Some(delvec_meta) = schema_change.delvec_meta.as_ref() {
        metadata.delvec_meta = Some(delvec_meta.clone());
    }
    Ok(())
}

fn maybe_update_storage_schema_for_write(
    metadata: &mut crate::connector::starrocks::lake::storage_domain::StorageTabletMetadata,
    write: &crate::connector::starrocks::lake::storage_domain::StorageWriteOperation,
    default_schema_id: i64,
    runtime_schema: &StarRocksTabletSchema,
    tablet_id: Option<i64>,
    txn_id: Option<i64>,
) -> Result<i64, String> {
    let schema_id = write
        .schema_key
        .as_ref()
        .ok_or_else(|| format!("op_write missing schema_key for publish_version: tablet_id={tablet_id:?} txn_id={txn_id:?}"))?
        .schema_id
        .filter(|value| *value > 0)
        .unwrap_or(default_schema_id);
    if schema_id <= 0 {
        return Err(format!(
            "op_write has non-positive schema_id after fallback: tablet_id={tablet_id:?} txn_id={txn_id:?} schema_id={schema_id} default_schema_id={default_schema_id}"
        ));
    }
    let current_schema_id = metadata
        .schema
        .as_ref()
        .and_then(|schema| schema.id)
        .unwrap_or(0);
    if current_schema_id == schema_id || metadata.historical_schemas.contains_key(&schema_id) {
        return Ok(schema_id);
    }
    let resolved_schema = if runtime_schema.id == Some(schema_id) {
        Some(runtime_schema.clone())
    } else {
        metadata.historical_schemas.get(&schema_id).cloned()
    }
    .ok_or_else(|| format!(
        "publish_version cannot resolve schema from schema_key: tablet_id={tablet_id:?} txn_id={txn_id:?} schema_id={schema_id} runtime_schema_id={:?} historical_schema_ids={:?}",
        runtime_schema.id,
        metadata.historical_schemas.keys().collect::<Vec<_>>()
    ))?;
    apply_storage_tablet_schema_update(
        metadata,
        &resolved_schema,
        format!("op_write schema update: tablet_id={tablet_id:?} txn_id={txn_id:?}"),
    )?;
    Ok(schema_id)
}

fn apply_storage_tablet_schema_update(
    metadata: &mut crate::connector::starrocks::lake::storage_domain::StorageTabletMetadata,
    new_schema: &StarRocksTabletSchema,
    reason: String,
) -> Result<(), String> {
    let schema_id = new_schema
        .id
        .ok_or_else(|| format!("tablet schema update missing schema id: reason={reason}"))?;
    if schema_id <= 0 {
        return Err(format!(
            "tablet schema update has non-positive schema id: reason={reason} schema_id={schema_id}"
        ));
    }
    if let Some(existing_schema) = metadata.schema.as_ref() {
        let existing_schema_id = existing_schema.id.unwrap_or(0);
        if existing_schema_id == schema_id {
            metadata
                .historical_schemas
                .entry(schema_id)
                .or_insert_with(|| existing_schema.clone());
            return Ok(());
        }
        if existing_schema_id > 0 {
            if metadata.rowset_to_schema.is_empty() && !metadata.rowsets.is_empty() {
                for rowset in &metadata.rowsets {
                    let rowset_id = rowset.id.ok_or_else(|| format!(
                        "tablet rowset id is missing when switching schema: reason={reason} existing_schema_id={existing_schema_id}"
                    ))?;
                    metadata
                        .rowset_to_schema
                        .entry(rowset_id)
                        .or_insert(existing_schema_id);
                }
            }
            metadata
                .historical_schemas
                .entry(existing_schema_id)
                .or_insert_with(|| existing_schema.clone());
        }
    } else if !metadata.rowsets.is_empty() {
        for rowset in &metadata.rowsets {
            let rowset_id = rowset.id.ok_or_else(|| format!(
                "tablet rowset id is missing when recovering schema-less metadata: reason={reason}"
            ))?;
            metadata
                .rowset_to_schema
                .entry(rowset_id)
                .or_insert(schema_id);
        }
    }
    metadata
        .historical_schemas
        .entry(schema_id)
        .or_insert_with(|| new_schema.clone());
    metadata.schema = Some(new_schema.clone());
    Ok(())
}

fn apply_storage_alter_metadata_log(
    metadata: &mut crate::connector::starrocks::lake::storage_domain::StorageTabletMetadata,
    alter_metadata: &crate::connector::starrocks::lake::storage_domain::StorageAlterMetadataOperation,
    txn_log: &crate::connector::starrocks::lake::storage_domain::StorageTransactionLog,
) -> Result<(), String> {
    for update in &alter_metadata.metadata_updates {
        if let Some(value) = update.enable_persistent_index {
            metadata.enable_persistent_index = Some(value);
        }
        if let Some(value) = update.persistent_index_type {
            metadata.persistent_index_type = Some(value);
        }
        if let Some(value) = update.compaction_strategy {
            metadata.compaction_strategy = Some(value);
        }
        if let Some(value) = update.flat_json_config.as_ref() {
            metadata.flat_json_config = Some(value.clone());
        }
        if let Some(schema) = update.tablet_schema.as_ref() {
            apply_storage_tablet_schema_update(
                metadata,
                schema,
                format!(
                    "op_alter_metadata schema update: tablet_id={:?} txn_id={:?}",
                    txn_log.tablet_id, txn_log.txn_id
                ),
            )?;
        }
    }
    Ok(())
}

fn next_storage_rowset_id(
    rowsets: &[crate::connector::starrocks::lake::storage_domain::StorageRowset],
) -> u32 {
    rowsets
        .iter()
        .filter_map(|rowset| rowset.id)
        .max()
        .map(|value| value.saturating_add(1))
        .unwrap_or(0)
}

fn normalize_storage_rowset(
    rowset: &mut crate::connector::starrocks::lake::storage_domain::StorageRowset,
) -> Result<(), String> {
    if !rowset.segment_metas.is_empty() && rowset.segment_metas.len() != rowset.segments.len() {
        return Err(format!(
            "rowset segment_metas/segments length mismatch: segment_metas={} segments={}",
            rowset.segment_metas.len(),
            rowset.segments.len()
        ));
    }
    rowset.shared_segments.resize(rowset.segments.len(), false);
    rowset.shared_segments.truncate(rowset.segments.len());
    Ok(())
}

fn storage_primary_key_schema_change_requires_delvec_support(
    metadata: &crate::connector::starrocks::lake::storage_domain::StorageTabletMetadata,
    tablet_schema: &StarRocksTabletSchema,
) -> Result<bool, String> {
    if !is_primary_keys_table(tablet_schema)? {
        return Ok(false);
    }
    Ok(metadata
        .delvec_meta
        .as_ref()
        .is_some_and(|meta| !meta.version_to_file.is_empty() || !meta.delvecs.is_empty())
        || metadata
            .rowsets
            .iter()
            .any(|rowset| !rowset.del_files.is_empty()))
}

#[cfg(test)]
mod storage_domain_tests {
    use super::*;
    use crate::connector::starrocks::lake::storage_domain::{
        StorageAlterMetadataOperation, StorageCompactionOperation, StorageFlatJsonConfig,
        StorageMetadataUpdate, StorageRowset, StorageSchemaKey, StorageTabletMetadata,
        StorageTransactionLog, StorageWriteOperation,
    };
    use crate::connector::starrocks::schema::{
        StarRocksColumnSchema, StarRocksKeysType, StarRocksTabletSchema,
    };

    fn schema() -> StarRocksTabletSchema {
        StarRocksTabletSchema {
            id: Some(7),
            keys_type: Some(StarRocksKeysType::Duplicate),
            column: vec![StarRocksColumnSchema {
                unique_id: 1,
                name: Some("k1".to_string()),
                r#type: "BIGINT".to_string(),
                is_key: Some(true),
                ..StarRocksColumnSchema::default()
            }],
            ..StarRocksTabletSchema::default()
        }
    }

    #[test]
    fn storage_domain_write_then_compaction_preserves_rowset_contract() {
        let schema = schema();
        let mut metadata = StorageTabletMetadata {
            schema: Some(schema.clone()),
            next_rowset_id: Some(3),
            rowset_to_schema: std::collections::HashMap::from([(1, 7), (2, 7)]),
            rowsets: vec![
                StorageRowset {
                    id: Some(1),
                    segments: vec!["a.dat".to_string()],
                    num_rows: Some(4),
                    ..StorageRowset::default()
                },
                StorageRowset {
                    id: Some(2),
                    segments: vec!["b.dat".to_string()],
                    num_rows: Some(6),
                    ..StorageRowset::default()
                },
            ],
            ..StorageTabletMetadata::default()
        };
        let write = StorageTransactionLog {
            tablet_id: Some(9),
            txn_id: Some(11),
            write: Some(StorageWriteOperation {
                schema_key: Some(StorageSchemaKey {
                    schema_id: Some(7),
                    ..StorageSchemaKey::default()
                }),
                rowset: Some(StorageRowset {
                    segments: vec!["c.dat".to_string()],
                    num_rows: Some(8),
                    ..StorageRowset::default()
                }),
                ..StorageWriteOperation::default()
            }),
            ..StorageTransactionLog::default()
        };
        apply_storage_txn_log_to_metadata(&mut metadata, &write, 7, &schema, "", None, 3)
            .expect("apply write");
        assert_eq!(
            metadata.rowsets.last().and_then(|rowset| rowset.id),
            Some(3)
        );
        assert_eq!(metadata.next_rowset_id, Some(4));

        let compaction = StorageTransactionLog {
            tablet_id: Some(9),
            txn_id: Some(12),
            compaction: Some(StorageCompactionOperation {
                input_rowsets: vec![1, 2],
                output_rowset: Some(StorageRowset {
                    segments: vec!["compact.dat".to_string()],
                    num_rows: Some(10),
                    ..StorageRowset::default()
                }),
                ..StorageCompactionOperation::default()
            }),
            ..StorageTransactionLog::default()
        };
        apply_storage_txn_log_to_metadata(&mut metadata, &compaction, 7, &schema, "", None, 4)
            .expect("apply compaction");
        assert_eq!(metadata.rowsets.len(), 2);
        assert_eq!(metadata.rowsets[0].id, Some(4));
        assert_eq!(metadata.rowsets[0].max_compact_input_rowset_id, Some(2));
        assert_eq!(metadata.rowset_to_schema.get(&4), Some(&7));
    }

    #[test]
    fn storage_domain_alter_metadata_updates_typed_facts() {
        let schema = schema();
        let mut metadata = StorageTabletMetadata::default();
        let log = StorageTransactionLog {
            tablet_id: Some(9),
            txn_id: Some(13),
            alter_metadata: Some(StorageAlterMetadataOperation {
                metadata_updates: vec![StorageMetadataUpdate {
                    enable_persistent_index: Some(true),
                    persistent_index_type: Some(2),
                    compaction_strategy: Some(3),
                    flat_json_config: Some(StorageFlatJsonConfig {
                        enabled: Some(true),
                        ..StorageFlatJsonConfig::default()
                    }),
                    tablet_schema: Some(schema.clone()),
                    ..StorageMetadataUpdate::default()
                }],
            }),
            ..StorageTransactionLog::default()
        };
        apply_storage_txn_log_to_metadata(&mut metadata, &log, 7, &schema, "", None, 3)
            .expect("apply alter metadata");
        assert_eq!(metadata.schema, Some(schema));
        assert_eq!(metadata.enable_persistent_index, Some(true));
        assert_eq!(metadata.compaction_strategy, Some(3));
        assert_eq!(
            metadata.flat_json_config.and_then(|config| config.enabled),
            Some(true)
        );
    }
}
