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

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Int8Array, Int16Array,
    Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use roaring::RoaringBitmap;
use sha2::{Digest, Sha256};

use crate::connector::starrocks::ObjectStoreProfile;
use crate::connector::starrocks::lake::txn_log::{
    ensure_rowset_segment_meta_consistency, normalize_rowset_shared_segments,
};
use crate::connector::starrocks::schema::{
    StarRocksColumnSchema, StarRocksKeysType, StarRocksTabletSchema,
};
use crate::formats::starrocks::metadata::{
    StarRocksSegmentFile, StarRocksTabletSnapshot, load_bundle_segment_footers,
    tablet_operator_relative_path,
};
use crate::formats::starrocks::plan::build_native_read_plan;
use crate::formats::starrocks::reader::build_native_record_batch;
use crate::formats::starrocks::writer::bundle_meta::next_rowset_id;
use crate::formats::starrocks::writer::io::{read_bytes, write_bytes};
use crate::formats::starrocks::writer::layout::{DATA_DIR, join_tablet_path};
use crate::formats::starrocks::writer::read_bundle_parquet_snapshot_if_any;
use crate::runtime::starlet_shard_registry::S3StoreConfig;
use crate::service::grpc_client::proto::starrocks::{
    DelfileWithRowsetId, DelvecMetadataPb, DelvecPagePb, FileMetaPb, RowsetMetadataPb,
    TabletMetadataPb, txn_log_pb,
};

const DELVEC_FORMAT_VERSION_V1: u8 = 0x01;
const CRC32C_MASK_DELTA: u32 = 0xa282_ead8;
const SLICE_ESCAPE_BYTE: u8 = 0x00;
const SLICE_ESCAPE_SUFFIX: u8 = 0x01;

#[derive(Clone, Copy)]
struct SegmentRowRef {
    segment_id: u32,
    row_id: u32,
}

struct SegmentKeyRows {
    segment_id: u32,
    keys: Vec<Vec<u8>>,
}

pub(crate) fn apply_primary_key_write_log_to_metadata(
    metadata: &mut TabletMetadataPb,
    op_write: &txn_log_pb::OpWrite,
    schema_id: i64,
    tablet_schema: &StarRocksTabletSchema,
    tablet_root_path: &str,
    s3_config: Option<&S3StoreConfig>,
    apply_version: i64,
    txn_id: i64,
) -> Result<(), String> {
    if apply_version <= 0 {
        return Err(format!(
            "invalid apply_version for primary key publish: {}",
            apply_version
        ));
    }
    if txn_id <= 0 {
        return Err(format!(
            "invalid txn_id for primary key publish: {}",
            txn_id
        ));
    }

    let mut maybe_new_rowset: Option<RowsetMetadataPb> = None;
    let mut rowset_id_for_append: Option<u32> = None;
    if let Some(rowset) = op_write.rowset.as_ref() {
        if rowset.delete_predicate.is_some() {
            return Err(
                "primary key publish does not support rowset delete_predicate yet".to_string(),
            );
        }
    } else if !op_write.dels.is_empty() {
        return Err(
            "primary key publish requires op_write.rowset when op_write.dels is non-empty"
                .to_string(),
        );
    } else {
        return Ok(());
    }

    let mut working = metadata.clone();
    if let Some(rowset) = op_write.rowset.as_ref() {
        let should_append_rowset = rowset.num_rows.unwrap_or(0) > 0 || !op_write.dels.is_empty();
        if should_append_rowset {
            let mut new_rowset = rowset.clone();
            ensure_rowset_segment_meta_consistency(&new_rowset)?;
            normalize_rowset_shared_segments(&mut new_rowset);
            let rowset_id = working
                .next_rowset_id
                .unwrap_or_else(|| next_rowset_id(&working.rowsets));
            new_rowset.id = Some(rowset_id);
            attach_op_write_delete_files_to_rowset(&mut new_rowset, rowset_id, op_write)?;
            rowset_id_for_append = Some(rowset_id);
            maybe_new_rowset = Some(new_rowset);
        }
    }

    if maybe_new_rowset.is_none() && op_write.dels.is_empty() {
        return Ok(());
    }

    let key_output_schema = build_primary_key_output_schema(tablet_schema)?;

    let mut visible_index = build_visible_primary_key_index(
        &working,
        tablet_schema,
        tablet_root_path,
        s3_config,
        &key_output_schema,
    )?;
    if pkdbg_enabled() {
        tracing::info!(
            "PKDBG primary-key publish start: txn_id={} apply_version={} root={} visible_key_count={} incoming_dels={} incoming_rowset_rows={}",
            txn_id,
            apply_version,
            tablet_root_path,
            visible_index.len(),
            op_write.dels.len(),
            maybe_new_rowset
                .as_ref()
                .and_then(|v| v.num_rows)
                .unwrap_or(0)
        );
    }

    let mut changed_deletes: HashMap<u32, RoaringBitmap> = HashMap::new();

    // Apply the new rowset's rows to the visible index BEFORE the op_write
    // deletes. A Mixed-routing batch (UPSERT then DELETE on the same key)
    // is split by the writer into `upsert_batch` (in op_write.rowset) and
    // `delete_key_batch` (in op_write.dels); to preserve the user-intended
    // ordering where a same-batch DELETE shadows the just-inserted UPSERT,
    // the new rowset's rows must enter visible_index first, so the DELETE
    // can find them and record a delvec bit on the new rowset's own segment.
    if let (Some(new_rowset), Some(rowset_id)) = (&maybe_new_rowset, rowset_id_for_append)
        && new_rowset.num_rows.unwrap_or(0) > 0
    {
        let new_segment_keys = scan_rowset_primary_key_rows(
            new_rowset,
            rowset_id,
            tablet_schema,
            tablet_root_path,
            s3_config,
            &key_output_schema,
        )?;
        for segment in &new_segment_keys {
            for (row_idx, key) in segment.keys.iter().enumerate() {
                let row_id = u32::try_from(row_idx).map_err(|_| {
                    format!(
                        "row id overflow while applying primary key rowset: segment_id={}, row_index={}",
                        segment.segment_id, row_idx
                    )
                })?;
                if let Some(old_ref) = visible_index.insert(
                    key.clone(),
                    SegmentRowRef {
                        segment_id: segment.segment_id,
                        row_id,
                    },
                ) {
                    changed_deletes
                        .entry(old_ref.segment_id)
                        .or_default()
                        .insert(old_ref.row_id);
                }
            }
        }
    }

    if !op_write.dels.is_empty() {
        let delete_keys =
            load_delete_keys_from_op_write_files(op_write, tablet_root_path, &key_output_schema)?;
        let mut delete_hits = 0usize;
        let mut delete_misses = 0usize;
        if pkdbg_enabled() {
            tracing::info!(
                "PKDBG primary-key delete payload decoded: txn_id={} apply_version={} root={} delete_key_count={} delete_key_preview={:?}",
                txn_id,
                apply_version,
                tablet_root_path,
                delete_keys.len(),
                preview_encoded_keys(&delete_keys, 8)
            );
        }
        for key in delete_keys {
            if let Some(old_ref) = visible_index.remove(&key) {
                delete_hits = delete_hits.saturating_add(1);
                changed_deletes
                    .entry(old_ref.segment_id)
                    .or_default()
                    .insert(old_ref.row_id);
            } else {
                delete_misses = delete_misses.saturating_add(1);
            }
        }
        if pkdbg_enabled() {
            tracing::info!(
                "PKDBG primary-key delete apply summary: txn_id={} apply_version={} root={} hit={} miss={} delvec_segments_to_update={}",
                txn_id,
                apply_version,
                tablet_root_path,
                delete_hits,
                delete_misses,
                changed_deletes.len()
            );
        }
    }

    if !changed_deletes.is_empty() {
        let mut merged_updates: HashMap<u32, RoaringBitmap> = HashMap::new();
        for (segment_id, added) in changed_deletes {
            let mut base = load_segment_delvec_bitmap(&working, tablet_root_path, segment_id)?;
            base |= added;
            merged_updates.insert(segment_id, base);
        }
        persist_delvec_updates(
            &mut working,
            tablet_root_path,
            apply_version,
            txn_id,
            &merged_updates,
        )?;
    }

    if let (Some(new_rowset), Some(rowset_id)) = (maybe_new_rowset, rowset_id_for_append) {
        working
            .next_rowset_id
            .replace(rowset_id.saturating_add(std::cmp::max(1, new_rowset.segments.len()) as u32));
        if !working.rowset_to_schema.is_empty() {
            working.rowset_to_schema.insert(rowset_id, schema_id);
        }
        working.rowsets.push(new_rowset);
    }

    *metadata = working;
    Ok(())
}

fn pkdbg_enabled() -> bool {
    match std::env::var("NOVAROCKS_PKDBG") {
        Ok(raw) => matches!(
            raw.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => false,
    }
}

fn preview_encoded_keys(keys: &[Vec<u8>], limit: usize) -> Vec<String> {
    keys.iter().take(limit).map(hex::encode).collect()
}

fn build_primary_key_output_schema(
    tablet_schema: &StarRocksTabletSchema,
) -> Result<SchemaRef, String> {
    let mut fields = Vec::new();
    for column in &tablet_schema.column {
        if !column.is_key.unwrap_or(false) {
            continue;
        }
        let name = column
            .name
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                format!(
                    "primary key column missing name in tablet schema: unique_id={}",
                    column.unique_id
                )
            })?;
        let data_type = primary_key_arrow_type(column)?;
        fields.push(Field::new(
            name,
            data_type,
            column.is_nullable.unwrap_or(false),
        ));
    }
    if fields.is_empty() {
        return Err("tablet schema has no key columns for primary key publish".to_string());
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn primary_key_arrow_type(column: &StarRocksColumnSchema) -> Result<DataType, String> {
    let raw = column.r#type.trim().to_ascii_uppercase();
    match raw.as_str() {
        "BOOLEAN" => Ok(DataType::Boolean),
        "TINYINT" => Ok(DataType::Int8),
        "SMALLINT" => Ok(DataType::Int16),
        "INT" => Ok(DataType::Int32),
        "BIGINT" => Ok(DataType::Int64),
        "DATE" | "DATE_V2" => Ok(DataType::Date32),
        "DATETIME" | "DATETIME_V2" | "TIMESTAMP" => {
            Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
        }
        "CHAR" | "VARCHAR" | "STRING" => Ok(DataType::Utf8),
        "BINARY" | "VARBINARY" => Ok(DataType::Binary),
        "DECIMAL128" => Err(format!(
            "unsupported primary key column type in v1 publish path: type=DECIMAL128, unique_id={}",
            column.unique_id
        )),
        other => Err(format!(
            "unsupported primary key column type in v1 publish path: type={}, unique_id={}",
            other, column.unique_id
        )),
    }
}

fn build_visible_primary_key_index(
    metadata: &TabletMetadataPb,
    tablet_schema: &StarRocksTabletSchema,
    tablet_root_path: &str,
    s3_config: Option<&S3StoreConfig>,
    key_output_schema: &SchemaRef,
) -> Result<HashMap<Vec<u8>, SegmentRowRef>, String> {
    let mut out = HashMap::new();
    for rowset in &metadata.rowsets {
        if rowset.segments.is_empty() {
            continue;
        }
        let rowset_id = rowset.id.ok_or_else(|| {
            "primary key publish requires rowset.id for existing rowset".to_string()
        })?;
        let segment_keys = scan_rowset_primary_key_rows(
            rowset,
            rowset_id,
            tablet_schema,
            tablet_root_path,
            s3_config,
            key_output_schema,
        )?;
        for segment in segment_keys {
            let delvec =
                load_segment_delvec_bitmap(metadata, tablet_root_path, segment.segment_id)?;
            for (row_idx, key) in segment.keys.into_iter().enumerate() {
                let row_id = u32::try_from(row_idx).map_err(|_| {
                    format!(
                        "row id overflow while building primary key index: segment_id={}, row_index={}",
                        segment.segment_id, row_idx
                    )
                })?;
                if delvec.contains(row_id) {
                    continue;
                }
                out.insert(
                    key,
                    SegmentRowRef {
                        segment_id: segment.segment_id,
                        row_id,
                    },
                );
            }
        }
    }
    Ok(out)
}

fn scan_rowset_primary_key_rows(
    rowset: &RowsetMetadataPb,
    rowset_id: u32,
    tablet_schema: &StarRocksTabletSchema,
    tablet_root_path: &str,
    s3_config: Option<&S3StoreConfig>,
    key_output_schema: &SchemaRef,
) -> Result<Vec<SegmentKeyRows>, String> {
    if rowset.segments.is_empty() {
        return Ok(Vec::new());
    }
    let mut output = Vec::with_capacity(rowset.segments.len());
    let object_store_profile = build_metadata_object_store_profile(tablet_root_path, s3_config)?;
    let mut snapshot_schema = tablet_schema.clone();
    snapshot_schema.keys_type = Some(StarRocksKeysType::Duplicate);
    let has_bundle_offsets = !rowset.bundle_file_offsets.is_empty();
    let has_segment_sizes = !rowset.segment_size.is_empty();

    for (idx, segment_name) in rowset.segments.iter().enumerate() {
        let bundle_file_offset = rowset.bundle_file_offsets.get(idx).copied();
        let segment_size = rowset.segment_size.get(idx).copied();
        if has_bundle_offsets && bundle_file_offset.is_none() {
            return Err(format!(
                "rowset bundle_file_offsets missing entry: segment_index={}, segment_count={}, offsets={}",
                idx,
                rowset.segments.len(),
                rowset.bundle_file_offsets.len()
            ));
        }
        if has_segment_sizes && segment_size.is_none() {
            return Err(format!(
                "rowset segment_size missing entry: segment_index={}, segment_count={}, segment_sizes={}",
                idx,
                rowset.segments.len(),
                rowset.segment_size.len()
            ));
        }
        if let Some(offset) = bundle_file_offset {
            if offset < 0 {
                return Err(format!(
                    "invalid negative bundle file offset while scanning primary key rowset: segment={}, offset={}",
                    segment_name, offset
                ));
            }
            if segment_size.is_none() || segment_size == Some(0) {
                return Err(format!(
                    "missing or zero segment_size for bundle segment while scanning primary key rowset: segment={}, offset={}",
                    segment_name, offset
                ));
            }
        }
        let relative_path = format!("{DATA_DIR}/{}", segment_name.trim_start_matches('/'));
        let path = join_tablet_path(tablet_root_path, &relative_path)?;
        let operator_relative_path =
            tablet_operator_relative_path(tablet_root_path, &relative_path)?;
        let segment_id = rowset_id
            .checked_add(u32::try_from(idx).map_err(|_| {
                format!(
                    "segment index overflow while scanning primary key rowset: index={}",
                    idx
                )
            })?)
            .ok_or_else(|| {
                format!(
                    "segment id overflow while scanning primary key rowset: rowset_id={}, segment_index={}",
                    rowset_id, idx
                )
            })?;
        let (cache_tablet_id, cache_version) = build_pk_scan_cache_identity(
            tablet_root_path,
            &relative_path,
            segment_id,
            bundle_file_offset,
            segment_size,
        );

        let schema_id = snapshot_schema.id.filter(|id| *id > 0);
        let historical_schemas = schema_id
            .map(|id| std::collections::BTreeMap::from([(id, snapshot_schema.clone())]))
            .unwrap_or_default();
        let snapshot = StarRocksTabletSnapshot {
            // Use a stable per-segment synthetic cache identity. Partition-root layouts
            // can share the same tablet_root_path across tablets, so rowset_id-based
            // synthetic keys are not unique enough and can cross-hit cache entries.
            tablet_id: cache_tablet_id,
            version: cache_version,
            metadata_path: String::new(),
            tablet_schema: snapshot_schema.clone(),
            historical_schemas,
            total_num_rows: 0,
            rowset_count: 1,
            segment_files: vec![StarRocksSegmentFile {
                name: segment_name.clone(),
                relative_path: operator_relative_path,
                path,
                rowset_version: rowset.version.unwrap_or(0),
                schema_id,
                segment_id: Some(segment_id),
                bundle_file_offset,
                segment_size,
            }],
            delete_predicates: Vec::new(),
            delvec_meta: Default::default(),
        };

        // PK publish must support rowsets written in parquet fallback format.
        // When parquet segment(s) are present, read them via parquet reader and
        // extract encoded primary keys directly, instead of forcing native path.
        if let Some(batch) =
            read_bundle_parquet_snapshot_if_any(&snapshot, key_output_schema.clone())?
        {
            let keys = encode_primary_keys_from_batch(&batch)?;
            output.push(SegmentKeyRows { segment_id, keys });
            continue;
        }

        let segment_footers = load_bundle_segment_footers(
            &snapshot,
            tablet_root_path,
            object_store_profile.as_ref(),
        )?;
        let plan = build_native_read_plan(&snapshot, &segment_footers, key_output_schema, None)?;
        let batch = build_native_record_batch(
            &plan,
            &segment_footers,
            tablet_root_path,
            object_store_profile.as_ref(),
            key_output_schema,
            &[],
        )?;
        let keys = encode_primary_keys_from_batch(&batch)?;
        output.push(SegmentKeyRows { segment_id, keys });
    }

    Ok(output)
}

fn build_pk_scan_cache_identity(
    tablet_root_path: &str,
    segment_relative_path: &str,
    segment_id: u32,
    bundle_file_offset: Option<i64>,
    segment_size: Option<u64>,
) -> (i64, i64) {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    tablet_root_path.hash(&mut hasher);
    segment_relative_path.hash(&mut hasher);
    segment_id.hash(&mut hasher);
    bundle_file_offset.hash(&mut hasher);
    segment_size.hash(&mut hasher);
    let id_bits = hasher.finish();

    let mut version_hasher = std::collections::hash_map::DefaultHasher::new();
    id_bits.hash(&mut version_hasher);
    0x50_4b_5f_53_43_41_4e_01_u64.hash(&mut version_hasher);
    let version_bits = version_hasher.finish();

    (
        i64::from_ne_bytes(id_bits.to_ne_bytes()),
        i64::from_ne_bytes(version_bits.to_ne_bytes()),
    )
}

fn build_metadata_object_store_profile(
    tablet_root_path: &str,
    s3_config: Option<&S3StoreConfig>,
) -> Result<Option<ObjectStoreProfile>, String> {
    crate::connector::starrocks::fs_access::object_store_profile_for_tablet_root(
        tablet_root_path,
        s3_config,
    )
}

fn encode_primary_keys_from_batch(batch: &RecordBatch) -> Result<Vec<Vec<u8>>, String> {
    let mut out = Vec::with_capacity(batch.num_rows());
    let single_key_mode = batch.num_columns() == 1;
    let last_col_idx = batch.num_columns().saturating_sub(1);
    for row_idx in 0..batch.num_rows() {
        let mut key = Vec::new();
        for col_idx in 0..batch.num_columns() {
            let array = batch.column(col_idx);
            encode_primary_key_cell(
                array.as_ref(),
                row_idx,
                col_idx == last_col_idx,
                single_key_mode,
                &mut key,
            )?;
        }
        out.push(key);
    }
    Ok(out)
}

fn encode_primary_key_cell(
    array: &dyn Array,
    row_idx: usize,
    is_last_col: bool,
    single_key_mode: bool,
    out: &mut Vec<u8>,
) -> Result<(), String> {
    if array.is_null(row_idx) {
        return Err(format!(
            "primary key row contains NULL value at row_idx={} for type {:?}",
            row_idx,
            array.data_type()
        ));
    }
    match array.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "downcast BOOLEAN array failed".to_string())?;
            out.push(u8::from(arr.value(row_idx)));
        }
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "downcast INT8 array failed".to_string())?;
            if single_key_mode {
                out.push(arr.value(row_idx) as u8);
            } else {
                let value = arr.value(row_idx) ^ i8::MIN;
                out.push(value as u8);
            }
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "downcast INT16 array failed".to_string())?;
            if single_key_mode {
                out.extend_from_slice(&arr.value(row_idx).to_le_bytes());
            } else {
                let value = (arr.value(row_idx) as u16) ^ (1_u16 << 15);
                out.extend_from_slice(&value.to_be_bytes());
            }
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "downcast INT32 array failed".to_string())?;
            if single_key_mode {
                out.extend_from_slice(&arr.value(row_idx).to_le_bytes());
            } else {
                let value = (arr.value(row_idx) as u32) ^ (1_u32 << 31);
                out.extend_from_slice(&value.to_be_bytes());
            }
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "downcast INT64 array failed".to_string())?;
            if single_key_mode {
                out.extend_from_slice(&arr.value(row_idx).to_le_bytes());
            } else {
                let value = (arr.value(row_idx) as u64) ^ (1_u64 << 63);
                out.extend_from_slice(&value.to_be_bytes());
            }
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| "downcast DATE32 array failed".to_string())?;
            if single_key_mode {
                out.extend_from_slice(&arr.value(row_idx).to_le_bytes());
            } else {
                let value = (arr.value(row_idx) as u32) ^ (1_u32 << 31);
                out.extend_from_slice(&value.to_be_bytes());
            }
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| "downcast TIMESTAMP_MICROSECOND array failed".to_string())?;
            if single_key_mode {
                out.extend_from_slice(&arr.value(row_idx).to_le_bytes());
            } else {
                let value = (arr.value(row_idx) as u64) ^ (1_u64 << 63);
                out.extend_from_slice(&value.to_be_bytes());
            }
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "downcast UTF8 array failed".to_string())?;
            if single_key_mode {
                out.extend_from_slice(arr.value(row_idx).as_bytes());
            } else {
                encode_slice_key_bytes(arr.value(row_idx).as_bytes(), is_last_col, out)?;
            }
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| "downcast BINARY array failed".to_string())?;
            if single_key_mode {
                out.extend_from_slice(arr.value(row_idx));
            } else {
                encode_slice_key_bytes(arr.value(row_idx), is_last_col, out)?;
            }
        }
        DataType::Decimal128(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "downcast DECIMAL128 array failed".to_string())?;
            let raw = arr.value(row_idx);
            if single_key_mode {
                out.extend_from_slice(&raw.to_le_bytes());
            } else {
                let value = (raw as u128) ^ (1_u128 << 127);
                out.extend_from_slice(&value.to_be_bytes());
            }
        }
        other => {
            return Err(format!(
                "unsupported primary key output type in v1 publish path: {:?}",
                other
            ));
        }
    }
    Ok(())
}

fn encode_slice_key_bytes(
    value: &[u8],
    is_last_col: bool,
    out: &mut Vec<u8>,
) -> Result<(), String> {
    if is_last_col {
        out.extend_from_slice(value);
        return Ok(());
    }

    for byte in value {
        if *byte == SLICE_ESCAPE_BYTE {
            out.push(SLICE_ESCAPE_BYTE);
            out.push(SLICE_ESCAPE_SUFFIX);
        } else {
            out.push(*byte);
        }
    }
    out.push(SLICE_ESCAPE_BYTE);
    out.push(SLICE_ESCAPE_BYTE);
    Ok(())
}

fn attach_op_write_delete_files_to_rowset(
    rowset: &mut RowsetMetadataPb,
    rowset_id: u32,
    op_write: &txn_log_pb::OpWrite,
) -> Result<(), String> {
    if op_write.dels.is_empty() {
        return Ok(());
    }
    if !op_write.del_encryption_metas.is_empty()
        && op_write.del_encryption_metas.len() != op_write.dels.len()
    {
        return Err(format!(
            "op_write del_encryption_metas/dels length mismatch: del_encryption_metas={} dels={}",
            op_write.del_encryption_metas.len(),
            op_write.dels.len()
        ));
    }

    let op_offset = u32::try_from(std::cmp::max(rowset.segments.len(), 1) - 1).map_err(|_| {
        format!(
            "rowset segment count overflow while building del_files metadata: segments={}",
            rowset.segments.len()
        )
    })?;
    for (idx, del_name) in op_write.dels.iter().enumerate() {
        let encryption_meta = if op_write.del_encryption_metas.len() == op_write.dels.len() {
            op_write.del_encryption_metas.get(idx).cloned()
        } else {
            None
        };
        rowset.del_files.push(DelfileWithRowsetId {
            name: Some(del_name.clone()),
            origin_rowset_id: Some(rowset_id),
            op_offset: Some(op_offset),
            encryption_meta,
            shared: None,
        });
    }
    Ok(())
}

fn load_delete_keys_from_op_write_files(
    op_write: &txn_log_pb::OpWrite,
    tablet_root_path: &str,
    key_output_schema: &SchemaRef,
) -> Result<Vec<Vec<u8>>, String> {
    if op_write.dels.is_empty() {
        return Ok(Vec::new());
    }
    if !op_write.del_encryption_metas.is_empty() {
        if op_write.del_encryption_metas.len() != op_write.dels.len() {
            return Err(format!(
                "op_write del_encryption_metas/dels length mismatch: del_encryption_metas={} dels={}",
                op_write.del_encryption_metas.len(),
                op_write.dels.len()
            ));
        }
        for (idx, meta) in op_write.del_encryption_metas.iter().enumerate() {
            if !meta.is_empty() {
                return Err(format!(
                    "encrypted op_write.dels is not supported in primary key publish path yet: del_index={}, encrypted=true",
                    idx
                ));
            }
        }
    }

    let mut keys = Vec::new();
    for (idx, del_name) in op_write.dels.iter().enumerate() {
        let relative_path = format!("{DATA_DIR}/{}", del_name.trim_start_matches('/'));
        let path = join_tablet_path(tablet_root_path, &relative_path)?;
        let payload = read_bytes(&path)?;
        let mut decoded = decode_delete_keys_payload(&payload, key_output_schema).map_err(|e| {
            format!(
                "decode primary key delete file failed: del_index={}, del_file={}, error={}",
                idx, del_name, e
            )
        })?;
        keys.append(&mut decoded);
    }
    Ok(keys)
}

fn decode_delete_keys_payload(
    payload: &[u8],
    key_output_schema: &SchemaRef,
) -> Result<Vec<Vec<u8>>, String> {
    crate::connector::starrocks::lake::delete_payload_codec::decode_delete_keys_payload(
        payload,
        key_output_schema,
    )
}

fn load_segment_delvec_bitmap(
    metadata: &TabletMetadataPb,
    tablet_root_path: &str,
    segment_id: u32,
) -> Result<RoaringBitmap, String> {
    let Some(delvec_meta) = metadata.delvec_meta.as_ref() else {
        return Ok(RoaringBitmap::new());
    };
    let Some(page) = delvec_meta.delvecs.get(&segment_id) else {
        return Ok(RoaringBitmap::new());
    };
    let size = page.size.unwrap_or(0);
    if size == 0 {
        return Ok(RoaringBitmap::new());
    }
    let version = page.version.ok_or_else(|| {
        format!(
            "delvec page missing version for segment_id={} in tablet metadata",
            segment_id
        )
    })?;
    let file_meta = delvec_meta.version_to_file.get(&version).ok_or_else(|| {
        format!(
            "delvec file mapping missing for segment_id={} version={}",
            segment_id, version
        )
    })?;
    let file_name = file_meta
        .name
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| {
            format!(
                "delvec file name missing for segment_id={} version={}",
                segment_id, version
            )
        })?;
    let path = join_tablet_path(tablet_root_path, &format!("{DATA_DIR}/{file_name}"))?;
    let bytes = read_bytes(&path)?;
    let offset = page.offset.unwrap_or(0);
    let end = offset.checked_add(size).ok_or_else(|| {
        format!(
            "delvec page range overflow: segment_id={}, offset={}, size={}",
            segment_id, offset, size
        )
    })?;
    let end_usize = usize::try_from(end).map_err(|_| {
        format!(
            "delvec page end overflow: segment_id={}, end={}",
            segment_id, end
        )
    })?;
    let offset_usize = usize::try_from(offset).map_err(|_| {
        format!(
            "delvec page offset overflow: segment_id={}, offset={}",
            segment_id, offset
        )
    })?;
    if end_usize > bytes.len() || offset_usize > end_usize {
        return Err(format!(
            "delvec page out of file range: segment_id={}, offset={}, size={}, file_size={}",
            segment_id,
            offset,
            size,
            bytes.len()
        ));
    }
    let payload = &bytes[offset_usize..end_usize];

    if let Some(masked) = page.crc32c
        && page.crc32c_gen_version == Some(version)
    {
        let expected = crc32c_unmask(masked);
        let actual = crc32c::crc32c(payload);
        if expected != actual {
            return Err(format!(
                "delvec crc32c mismatch: segment_id={}, version={}, expected={}, actual={}",
                segment_id, version, expected, actual
            ));
        }
    }

    decode_delvec_bitmap(payload, segment_id, version)
}

fn persist_delvec_updates(
    metadata: &mut TabletMetadataPb,
    tablet_root_path: &str,
    apply_version: i64,
    txn_id: i64,
    updates: &HashMap<u32, RoaringBitmap>,
) -> Result<(), String> {
    if updates.is_empty() {
        return Ok(());
    }

    let mut all_updates: HashMap<u32, RoaringBitmap> = updates
        .iter()
        .map(|(segment_id, bitmap)| (*segment_id, bitmap.clone()))
        .collect();
    if let Some(existing_meta) = metadata.delvec_meta.as_ref() {
        for (segment_id, page) in &existing_meta.delvecs {
            if page.version != Some(apply_version) || all_updates.contains_key(segment_id) {
                continue;
            }
            let bitmap = load_segment_delvec_bitmap(metadata, tablet_root_path, *segment_id)?;
            all_updates.insert(*segment_id, bitmap);
        }
    }

    let mut segment_ids = all_updates.keys().copied().collect::<Vec<_>>();
    segment_ids.sort_unstable();

    let mut file_bytes = Vec::new();
    let mut pages = HashMap::with_capacity(segment_ids.len());
    for segment_id in segment_ids {
        let bitmap = all_updates.get(&segment_id).ok_or_else(|| {
            format!(
                "internal error: missing delvec bitmap for segment_id={}",
                segment_id
            )
        })?;
        let payload = encode_delvec_bitmap(bitmap)?;
        let offset = u64::try_from(file_bytes.len()).map_err(|_| {
            format!(
                "delvec file offset overflow while writing segment_id={}",
                segment_id
            )
        })?;
        let size = u64::try_from(payload.len()).map_err(|_| {
            format!(
                "delvec payload size overflow while writing segment_id={}",
                segment_id
            )
        })?;
        let crc32c_masked = crc32c_mask(crc32c::crc32c(&payload));
        file_bytes.extend_from_slice(&payload);
        pages.insert(
            segment_id,
            DelvecPagePb {
                version: Some(apply_version),
                offset: Some(offset),
                size: Some(size),
                crc32c: Some(crc32c_masked),
                crc32c_gen_version: Some(apply_version),
            },
        );
    }

    let file_name =
        if let Some(existing) = existing_delvec_file_name_for_version(metadata, apply_version)? {
            existing
        } else {
            let tablet_id = metadata.id.ok_or_else(|| {
                "tablet metadata missing tablet id while generating delvec file name".to_string()
            })?;
            build_delvec_file_name(tablet_id, txn_id, apply_version)?
        };
    let file_path = join_tablet_path(tablet_root_path, &format!("{DATA_DIR}/{file_name}"))?;
    write_bytes(&file_path, file_bytes)?;

    let delvec_meta = working_delvec_meta(metadata);
    delvec_meta.version_to_file.insert(
        apply_version,
        FileMetaPb {
            name: Some(file_name),
            ..Default::default()
        },
    );
    for (segment_id, page) in pages {
        delvec_meta.delvecs.insert(segment_id, page);
    }
    Ok(())
}

fn existing_delvec_file_name_for_version(
    metadata: &TabletMetadataPb,
    apply_version: i64,
) -> Result<Option<String>, String> {
    let Some(delvec_meta) = metadata.delvec_meta.as_ref() else {
        return Ok(None);
    };
    let Some(file_meta) = delvec_meta.version_to_file.get(&apply_version) else {
        return Ok(None);
    };
    let Some(file_name) = file_meta
        .name
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
    else {
        return Err(format!(
            "delvec file name missing for existing version mapping: version={}",
            apply_version
        ));
    };
    Ok(Some(file_name.to_string()))
}

fn build_delvec_file_name(
    tablet_id: i64,
    txn_id: i64,
    apply_version: i64,
) -> Result<String, String> {
    if tablet_id <= 0 {
        return Err(format!(
            "invalid tablet_id for delvec file name generation: {}",
            tablet_id
        ));
    }
    if txn_id <= 0 {
        return Err(format!(
            "invalid txn_id for delvec file name generation: {}",
            txn_id
        ));
    }
    if apply_version <= 0 {
        return Err(format!(
            "invalid apply_version for delvec file name generation: {}",
            apply_version
        ));
    }
    let seed = format!("delvec_file:tablet={tablet_id}:txn={txn_id}:version={apply_version}");
    let uuid = deterministic_uuid_v4_from_seed(&seed);
    Ok(format!("{:016x}_{}.delvec", txn_id as u64, uuid))
}

fn deterministic_uuid_v4_from_seed(seed: &str) -> String {
    let digest = Sha256::digest(seed.as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[0..16]);

    // Set RFC 4122 variant/version bits to UUIDv4 layout.
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;

    let hex = hex::encode(bytes);
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

fn working_delvec_meta(metadata: &mut TabletMetadataPb) -> &mut DelvecMetadataPb {
    metadata
        .delvec_meta
        .get_or_insert_with(DelvecMetadataPb::default)
}

fn encode_delvec_bitmap(bitmap: &RoaringBitmap) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    out.push(DELVEC_FORMAT_VERSION_V1);
    bitmap
        .serialize_into(&mut out)
        .map_err(|e| format!("serialize delvec bitmap failed: {}", e))?;
    Ok(out)
}

fn decode_delvec_bitmap(
    payload: &[u8],
    segment_id: u32,
    delvec_version: i64,
) -> Result<RoaringBitmap, String> {
    if payload.is_empty() {
        return Err(format!(
            "invalid delvec payload (empty): segment_id={}, version={}",
            segment_id, delvec_version
        ));
    }
    if payload[0] != DELVEC_FORMAT_VERSION_V1 {
        return Err(format!(
            "invalid delvec payload format: segment_id={}, version={}, flag={}",
            segment_id, delvec_version, payload[0]
        ));
    }
    if payload.len() == 1 {
        return Ok(RoaringBitmap::new());
    }
    let mut cursor = Cursor::new(&payload[1..]);
    RoaringBitmap::deserialize_from(&mut cursor).map_err(|e| {
        format!(
            "decode delvec roaring bitmap failed: segment_id={}, version={}, error={}",
            segment_id, delvec_version, e
        )
    })
}

fn crc32c_mask(crc: u32) -> u32 {
    crc.rotate_left(17).wrapping_add(CRC32C_MASK_DELTA)
}

fn crc32c_unmask(masked: u32) -> u32 {
    masked.wrapping_sub(CRC32C_MASK_DELTA).rotate_right(17)
}
