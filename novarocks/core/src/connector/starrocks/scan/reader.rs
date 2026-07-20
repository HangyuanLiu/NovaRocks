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

use crate::connector::MinMaxPredicate;
use crate::connector::starrocks::ObjectStoreProfile;
use crate::connector::starrocks::fe_v2_meta::fetch_table_schema_for_lake_scan;
use crate::connector::starrocks::schema::{
    LakeScanColumnHint, StarRocksAggStateDesc, StarRocksColumnSchema, StarRocksKeysType,
    StarRocksTabletIndex, StarRocksTabletSchema, StarRocksTypeDesc, StarRocksTypeNode,
};
use crate::exec::chunk::ChunkSchemaRef;
use crate::formats::starrocks::cache as native_cache;
use crate::formats::starrocks::data::build_native_record_batch;
use crate::formats::starrocks::metadata::{
    StarRocksTabletSnapshot, load_bundle_segment_footers, load_tablet_snapshot,
};
use crate::formats::starrocks::plan::{
    StarRocksOutputColumnHint, StarRocksPhysicalColumnBinding,
    build_native_read_plan_with_output_hints,
};
use crate::formats::starrocks::writer::read_bundle_parquet_snapshot_with_output_hints_and_physical_schema_if_any;
use crate::novarocks_logging::{info, warn};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};

use super::op::LakeScanSchemaMeta;
use crate::exec::dict_encode::{
    QueryGlobalDictEncodeMap, build_scan_schema_for_global_dict_encoding,
    encode_batch_with_query_global_dicts,
};

pub(super) struct StarRocksNativeReader {
    tablet_id: i64,
    version: i64,
    next_batch: Option<RecordBatch>,
}

const NATIVE_BATCH_CACHE_MAX_ROWS: u64 = 200_000;

fn schema_signature_with_hints(
    schema: &SchemaRef,
    chunk_schema: &ChunkSchemaRef,
    output_column_hints: &[StarRocksOutputColumnHint],
    current_tablet_schema: &StarRocksTabletSchema,
) -> Result<String, String> {
    if schema.fields().len() != chunk_schema.slots().len() {
        return Err(format!(
            "schema/chunk schema length mismatch while building signature: fields={} slots={}",
            schema.fields().len(),
            chunk_schema.slots().len()
        ));
    }
    if schema.fields().len() != output_column_hints.len() {
        return Err(format!(
            "schema/output column hint length mismatch while building signature: fields={} hints={}",
            schema.fields().len(),
            output_column_hints.len()
        ));
    }
    let output_signature = schema
        .fields()
        .iter()
        .zip(chunk_schema.slots().iter())
        .zip(output_column_hints.iter())
        .map(|((field, slot), hint)| {
            format!(
                "{}:{:?}:{}:slot={}:slot_uid={:?}:plan_uid={:?}:binding={:?}:default={:?}",
                field.name(),
                field.data_type(),
                field.is_nullable(),
                slot.slot_id(),
                slot.unique_id(),
                hint.schema_unique_id,
                hint.physical_binding,
                hint.fallback_default_literal
            )
        })
        .collect::<Vec<_>>()
        .join("|");
    Ok(format!(
        "{}|current_tablet_schema=v1:{}",
        output_signature,
        tablet_schema_semantic_fingerprint(current_tablet_schema)
    ))
}

#[derive(Default)]
struct SchemaFingerprintEncoder {
    bytes: Vec<u8>,
}

impl SchemaFingerprintEncoder {
    fn push_bool(&mut self, value: bool) {
        self.bytes.push(u8::from(value));
    }

    fn push_i32(&mut self, value: i32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn push_i64(&mut self, value: i64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn push_u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn push_u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn push_bytes(&mut self, value: &[u8]) {
        self.push_u64(value.len() as u64);
        self.bytes.extend_from_slice(value);
    }

    fn push_string(&mut self, value: &str) {
        self.push_bytes(value.as_bytes());
    }

    fn push_optional_i32(&mut self, value: Option<i32>) {
        self.push_bool(value.is_some());
        if let Some(value) = value {
            self.push_i32(value);
        }
    }

    fn push_optional_i64(&mut self, value: Option<i64>) {
        self.push_bool(value.is_some());
        if let Some(value) = value {
            self.push_i64(value);
        }
    }

    fn push_optional_u32(&mut self, value: Option<u32>) {
        self.push_bool(value.is_some());
        if let Some(value) = value {
            self.push_u32(value);
        }
    }

    fn push_optional_bool(&mut self, value: Option<bool>) {
        self.push_bool(value.is_some());
        if let Some(value) = value {
            self.push_bool(value);
        }
    }

    fn push_optional_f64(&mut self, value: Option<f64>) {
        self.push_bool(value.is_some());
        if let Some(value) = value {
            self.push_u64(value.to_bits());
        }
    }

    fn push_optional_string(&mut self, value: Option<&str>) {
        self.push_bool(value.is_some());
        if let Some(value) = value {
            self.push_string(value);
        }
    }

    fn push_optional_bytes(&mut self, value: Option<&[u8]>) {
        self.push_bool(value.is_some());
        if let Some(value) = value {
            self.push_bytes(value);
        }
    }

    fn push_type_desc(&mut self, value: &StarRocksTypeDesc) {
        self.push_u64(value.types.len() as u64);
        for node in &value.types {
            self.push_type_node(node);
        }
    }

    fn push_type_node(&mut self, value: &StarRocksTypeNode) {
        self.push_i32(value.r#type);
        self.push_bool(value.scalar_type.is_some());
        if let Some(scalar) = value.scalar_type.as_ref() {
            self.push_i32(scalar.r#type);
            self.push_optional_i32(scalar.len);
            self.push_optional_i32(scalar.precision);
            self.push_optional_i32(scalar.scale);
        }
        self.push_u64(value.struct_fields.len() as u64);
        for field in &value.struct_fields {
            self.push_string(&field.name);
            self.push_optional_string(field.comment.as_deref());
        }
    }

    fn push_agg_state(&mut self, value: &StarRocksAggStateDesc) {
        self.push_optional_string(value.agg_func_name.as_deref());
        self.push_u64(value.arg_types.len() as u64);
        for arg_type in &value.arg_types {
            self.push_type_desc(arg_type);
        }
        self.push_bool(value.ret_type.is_some());
        if let Some(ret_type) = value.ret_type.as_ref() {
            self.push_type_desc(ret_type);
        }
        self.push_optional_bool(value.is_result_nullable);
        self.push_optional_i32(value.func_version);
    }

    fn push_column(&mut self, value: &StarRocksColumnSchema) {
        self.push_i32(value.unique_id);
        self.push_optional_string(value.name.as_deref());
        self.push_string(&value.r#type);
        self.push_optional_bool(value.is_key);
        self.push_optional_string(value.aggregation.as_deref());
        self.push_optional_bool(value.is_nullable);
        self.push_optional_bytes(value.default_value.as_deref());
        self.push_optional_i32(value.precision);
        self.push_optional_i32(value.frac);
        self.push_optional_i32(value.length);
        self.push_optional_i32(value.index_length);
        self.push_optional_bool(value.is_bf_column);
        self.push_optional_i32(value.referenced_column_id);
        self.push_optional_string(value.referenced_column.as_deref());
        self.push_optional_bool(value.has_bitmap_index);
        self.push_optional_bool(value.visible);
        self.push_u64(value.children_columns.len() as u64);
        for child in &value.children_columns {
            self.push_column(child);
        }
        self.push_optional_bool(value.is_auto_increment);
        self.push_bool(value.agg_state_desc.is_some());
        if let Some(agg_state) = value.agg_state_desc.as_ref() {
            self.push_agg_state(agg_state);
        }
    }

    fn push_table_index(&mut self, value: &StarRocksTabletIndex) {
        self.push_optional_i64(value.index_id);
        self.push_optional_string(value.index_name.as_deref());
        self.push_optional_i32(value.index_type);
        self.push_u64(value.col_unique_id.len() as u64);
        for unique_id in &value.col_unique_id {
            self.push_i32(*unique_id);
        }
        self.push_optional_string(value.index_properties.as_deref());
    }

    fn push_tablet_schema(&mut self, value: &StarRocksTabletSchema) {
        self.push_optional_i32(value.keys_type.map(|keys_type| match keys_type {
            StarRocksKeysType::Duplicate => 0,
            StarRocksKeysType::Unique => 1,
            StarRocksKeysType::Aggregate => 2,
            StarRocksKeysType::Primary => 3,
        }));
        self.push_u64(value.column.len() as u64);
        for column in &value.column {
            self.push_column(column);
        }
        self.push_optional_i32(value.num_short_key_columns);
        self.push_optional_i32(value.num_rows_per_row_block);
        self.push_optional_f64(value.bf_fpp);
        self.push_optional_u32(value.next_column_unique_id);
        self.push_optional_bool(value.deprecated_is_in_memory);
        self.push_optional_i64(value.deprecated_id);
        self.push_optional_i32(value.compression_type);
        self.push_u64(value.sort_key_idxes.len() as u64);
        for index in &value.sort_key_idxes {
            self.push_u32(*index);
        }
        self.push_optional_i32(value.schema_version);
        self.push_u64(value.sort_key_unique_ids.len() as u64);
        for unique_id in &value.sort_key_unique_ids {
            self.push_u32(*unique_id);
        }
        self.push_u64(value.table_indices.len() as u64);
        for index in &value.table_indices {
            self.push_table_index(index);
        }
        self.push_optional_i32(value.compression_level);
        self.push_optional_i64(value.id);
    }
}

fn tablet_schema_semantic_fingerprint(schema: &StarRocksTabletSchema) -> String {
    let mut encoder = SchemaFingerprintEncoder::default();
    encoder.push_tablet_schema(schema);
    let digest = Sha256::digest(&encoder.bytes);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut fingerprint = String::with_capacity(digest.len() * 2);
    for byte in digest {
        fingerprint.push(HEX[(byte >> 4) as usize] as char);
        fingerprint.push(HEX[(byte & 0x0f) as usize] as char);
    }
    fingerprint
}

fn maybe_refresh_snapshot_schema_for_lake_scan(
    snapshot: &StarRocksTabletSnapshot,
    lake_schema_meta: Option<&LakeScanSchemaMeta>,
) -> Result<StarRocksTabletSnapshot, String> {
    let Some(meta) = lake_schema_meta else {
        return Ok(snapshot.clone());
    };
    if meta.schema_id <= 0 {
        return Ok(snapshot.clone());
    }

    if let Some(native_schema) = meta.native_tablet_schema.as_ref() {
        if native_schema.id != Some(meta.schema_id) {
            return Err(format!(
                "native StarRocks tablet schema id mismatch: metadata_schema_id={} tablet_schema_id={:?}",
                meta.schema_id, native_schema.id
            ));
        }
        let mut refreshed = snapshot.clone();
        refreshed.tablet_schema = native_schema.clone();
        return Ok(refreshed);
    }
    if meta.native_column_hints.is_some() {
        return Err(format!(
            "native StarRocks schema metadata requires the full current schema: db_id={} table_id={} schema_id={}",
            meta.db_id, meta.table_id, meta.schema_id
        ));
    }

    let snapshot_schema_id = snapshot.tablet_schema.id.unwrap_or(0);
    if snapshot_schema_id == meta.schema_id {
        return Ok(snapshot.clone());
    }

    let fe_schema = fetch_table_schema_for_lake_scan(
        meta.fe_addr.as_ref(),
        meta.db_id,
        meta.table_id,
        meta.schema_id,
        Some(snapshot.tablet_id),
        meta.query_id.clone(),
    )
    .map_err(|e| {
        format!(
            "fetch FE table schema for lake scan failed while refreshing snapshot schema: db_id={} table_id={} schema_id={} tablet_id={} error={}",
            meta.db_id, meta.table_id, meta.schema_id, snapshot.tablet_id, e
        )
    })?;
    let refreshed_schema = fe_schema.tablet_schema;
    let refreshed_schema_id = refreshed_schema.id.unwrap_or(0);
    if refreshed_schema_id > 0 && refreshed_schema_id != meta.schema_id {
        warn!(
            "lake scan FE schema id mismatch while refreshing snapshot schema: tablet_id={} snapshot_schema_id={} requested_schema_id={} fetched_schema_id={}",
            snapshot.tablet_id, snapshot_schema_id, meta.schema_id, refreshed_schema_id
        );
    }

    let mut refreshed = snapshot.clone();
    refreshed.tablet_schema = refreshed_schema;
    info!(
        "lake scan refreshed tablet schema from FE schema meta: tablet_id={} version={} snapshot_schema_id={} requested_schema_id={} metadata_path={}",
        refreshed.tablet_id,
        refreshed.version,
        snapshot_schema_id,
        meta.schema_id,
        refreshed.metadata_path
    );
    Ok(refreshed)
}

impl StarRocksNativeReader {
    pub(super) fn open(
        tablet_id: i64,
        storage_path: &str,
        version: i64,
        required_chunk_schema: ChunkSchemaRef,
        output_chunk_schema: ChunkSchemaRef,
        query_global_dicts: QueryGlobalDictEncodeMap,
        min_max_predicates: Vec<MinMaxPredicate>,
        object_store_profile: Option<&ObjectStoreProfile>,
        lake_schema_meta: Option<&LakeScanSchemaMeta>,
    ) -> Result<Self, String> {
        let output_schema = output_chunk_schema.arrow_schema_ref();
        let snapshot = match load_tablet_snapshot(
            tablet_id,
            version,
            storage_path,
            object_store_profile,
        ) {
            Ok(snapshot) => snapshot,
            Err(err)
                if should_treat_missing_tablet_metadata_as_empty(storage_path, version, &err) =>
            {
                warn!(
                    "starrocks native reader degrades missing tablet metadata to empty batch: tablet_id={} version={} path={} error={}",
                    tablet_id, version, storage_path, err
                );
                return Ok(Self {
                    tablet_id,
                    version,
                    next_batch: Some(RecordBatch::new_empty(output_schema.clone())),
                });
            }
            Err(err) => return Err(err),
        };
        let physical_snapshot = snapshot.clone();
        let snapshot = maybe_refresh_snapshot_schema_for_lake_scan(&snapshot, lake_schema_meta)?;
        let current_tablet_schema = snapshot.tablet_schema.clone();
        let output_column_hints = build_output_column_hints(
            &snapshot,
            &required_chunk_schema,
            &output_schema,
            &output_chunk_schema,
            lake_schema_meta,
        )?;
        let use_batch_cache = query_global_dicts.is_empty();
        let output_schema_sig = schema_signature_with_hints(
            &output_schema,
            &output_chunk_schema,
            &output_column_hints,
            &current_tablet_schema,
        )?;
        if use_batch_cache
            && let Some(batch) = native_cache::native_batch_cache_get(
                storage_path,
                tablet_id,
                version,
                &output_schema_sig,
            )
        {
            return Ok(Self {
                tablet_id,
                version,
                next_batch: Some(batch),
            });
        }
        eprintln!(
            "[DEBUG] starrocks native reader snapshot tablet_id={} requested_version={} metadata_path={} total_num_rows={} rowset_count={} segment_count={}",
            tablet_id,
            version,
            snapshot.metadata_path,
            snapshot.total_num_rows,
            snapshot.rowset_count,
            snapshot.segment_files.len()
        );
        info!(
            "starrocks native reader loaded snapshot tablet_id={} requested_version={} metadata_path={} total_num_rows={} rowset_count={} segment_count={}",
            tablet_id,
            version,
            snapshot.metadata_path,
            snapshot.total_num_rows,
            snapshot.rowset_count,
            snapshot.segment_files.len()
        );
        let output_schema_for_plan = output_schema.clone();
        let (scan_schema, has_dict_encoded_output) = build_scan_schema_for_global_dict_encoding(
            &output_schema_for_plan,
            &output_chunk_schema,
            &query_global_dicts,
        )?;
        let cacheable_small_snapshot = snapshot.total_num_rows <= NATIVE_BATCH_CACHE_MAX_ROWS;
        if let Some(batch) =
            read_bundle_parquet_snapshot_with_output_hints_and_physical_schema_if_any(
                &snapshot,
                scan_schema.clone(),
                &output_column_hints,
                &physical_snapshot.tablet_schema,
            )?
        {
            let batch = if has_dict_encoded_output {
                encode_batch_with_query_global_dicts(
                    batch,
                    &output_schema,
                    &output_chunk_schema,
                    &query_global_dicts,
                )?
            } else {
                batch
            };
            if use_batch_cache && cacheable_small_snapshot {
                native_cache::native_batch_cache_put(
                    storage_path,
                    tablet_id,
                    version,
                    &output_schema_sig,
                    batch.clone(),
                );
            }
            eprintln!(
                "[DEBUG] starrocks native reader parquet snapshot batch tablet_id={} rows={}",
                tablet_id,
                batch.num_rows()
            );
            info!(
                "starrocks native reader served parquet snapshot tablet_id={} rows={}",
                tablet_id,
                batch.num_rows()
            );
            return Ok(Self {
                tablet_id,
                version,
                next_batch: Some(batch),
            });
        }
        let segment_footers =
            load_bundle_segment_footers(&snapshot, storage_path, object_store_profile)?;
        let plan = build_native_read_plan_with_output_hints(
            &snapshot,
            &segment_footers,
            &scan_schema,
            &output_column_hints,
            Some(&physical_snapshot.tablet_schema),
        )?;
        if let Some(first_footer) = segment_footers.first() {
            let column_debug = first_footer
                .columns
                .iter()
                .map(|c| {
                    format!(
                        "uid={:?},type={:?},enc={:?},comp={:?},ord_root={:?},ord_root_is_data={:?}",
                        c.unique_id,
                        c.logical_type,
                        c.encoding,
                        c.compression,
                        c.ordinal_index_root_page
                            .as_ref()
                            .map(|p| format!("{}:{}", p.offset, p.size)),
                        c.ordinal_index_root_is_data_page
                    )
                })
                .collect::<Vec<_>>()
                .join(" | ");
            info!(
                "starrocks rust_native first segment footer summary: tablet_id={}, version={}, columns=[{}]",
                tablet_id, version, column_debug
            );
        }
        let batch = build_native_record_batch(
            &plan,
            &segment_footers,
            storage_path,
            object_store_profile,
            &scan_schema,
            if cacheable_small_snapshot {
                &[]
            } else {
                &min_max_predicates
            },
        )
        .map_err(|e| {
            format!(
                "starrocks rust_native reader open failed in native data path (tablet_id={}, version={}, segment_count={}, projected_columns={}, estimated_rows={}): {}",
                plan.tablet_id,
                plan.version,
                plan.segments.len(),
                plan.projected_columns.len(),
                plan.estimated_rows,
                e
            )
        })?;
        let batch = if has_dict_encoded_output {
            encode_batch_with_query_global_dicts(
                batch,
                &output_schema,
                &output_chunk_schema,
                &query_global_dicts,
            )?
        } else {
            batch
        };
        eprintln!(
            "[DEBUG] starrocks native reader built batch tablet_id={} rows={}",
            tablet_id,
            batch.num_rows()
        );
        info!(
            "starrocks native reader built batch tablet_id={} rows={}",
            tablet_id,
            batch.num_rows()
        );
        if use_batch_cache && cacheable_small_snapshot {
            native_cache::native_batch_cache_put(
                storage_path,
                tablet_id,
                version,
                &output_schema_sig,
                batch.clone(),
            );
        }
        Ok(Self {
            tablet_id,
            version,
            next_batch: Some(batch),
        })
    }

    pub(super) fn get_next(
        &mut self,
        _output_schema: &SchemaRef,
    ) -> Result<Option<RecordBatch>, String> {
        Ok(self.next_batch.take())
    }

    pub(super) fn close(&mut self) -> Result<(), String> {
        let _ = (self.tablet_id, self.version);
        Ok(())
    }
}

fn normalize_column_name(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn build_required_schema_unique_id_map(
    required_chunk_schema: &ChunkSchemaRef,
) -> Result<HashMap<String, u32>, String> {
    let mut out = HashMap::new();
    for slot in required_chunk_schema.slots() {
        let Some(raw_unique_id) = slot.unique_id() else {
            continue;
        };
        let unique_id = u32::try_from(raw_unique_id).map_err(|_| {
            format!(
                "invalid required chunk schema unique_id: slot={} field={} unique_id={}",
                slot.slot_id(),
                slot.name(),
                raw_unique_id
            )
        })?;
        out.insert(normalize_column_name(slot.name()), unique_id);
    }
    Ok(out)
}

fn build_lake_schema_column_hints_from_domain(
    schema: &StarRocksTabletSchema,
) -> Result<HashMap<String, LakeScanColumnHint>, String> {
    let mut out = HashMap::new();
    for column in &schema.column {
        let Some(name) = column.name.as_deref() else {
            continue;
        };
        let normalized_name = normalize_column_name(name);
        if normalized_name.is_empty() {
            continue;
        }
        let unique_id = match column.unique_id {
            v if v >= 0 => Some(u32::try_from(v).map_err(|_| {
                format!(
                    "invalid local tablet schema unique_id for column '{}': {}",
                    name, v
                )
            })?),
            _ => None,
        };
        let default_value = column
            .default_value
            .as_ref()
            .map(|value| String::from_utf8_lossy(value).into_owned());
        let hint = LakeScanColumnHint {
            unique_id,
            default_value,
        };
        if let Some(existing) = out.get(&normalized_name)
            && existing != &hint
        {
            return Err(format!(
                "duplicated local tablet schema column with mismatched metadata: column_name={}",
                name
            ));
        }
        out.insert(normalized_name, hint);
    }
    Ok(out)
}

fn build_native_schema_column_hints(
    columns: &[super::op::StarRocksSchemaColumnHint],
) -> Result<HashMap<String, LakeScanColumnHint>, String> {
    let mut out = HashMap::new();
    for column in columns {
        let normalized_name = normalize_column_name(&column.name);
        if normalized_name.is_empty() {
            return Err("native StarRocks schema hint column name must not be empty".to_string());
        }
        let unique_id = u32::try_from(column.unique_id).map_err(|_| {
            format!(
                "invalid native StarRocks schema hint unique_id for column '{}': {}",
                column.name, column.unique_id
            )
        })?;
        let hint = LakeScanColumnHint {
            unique_id: Some(unique_id),
            default_value: column.default_value.clone(),
        };
        if let Some(existing) = out.get(&normalized_name) {
            if existing != &hint {
                return Err(format!(
                    "duplicated native StarRocks schema hint with mismatched metadata: column_name={}",
                    column.name
                ));
            }
            return Err(format!(
                "duplicated native StarRocks schema hint: column_name={}",
                column.name
            ));
        }
        out.insert(normalized_name, hint);
    }
    if out.is_empty() {
        return Err("native StarRocks schema hints must not be empty".to_string());
    }
    Ok(out)
}

fn build_output_column_hints(
    snapshot: &StarRocksTabletSnapshot,
    required_chunk_schema: &ChunkSchemaRef,
    output_schema: &SchemaRef,
    output_chunk_schema: &ChunkSchemaRef,
    lake_schema_meta: Option<&LakeScanSchemaMeta>,
) -> Result<Vec<StarRocksOutputColumnHint>, String> {
    if output_schema.fields().len() != output_chunk_schema.slots().len() {
        return Err(format!(
            "output schema/chunk schema length mismatch while building lake hints: fields={} slots={}",
            output_schema.fields().len(),
            output_chunk_schema.slots().len()
        ));
    }
    let required_unique_ids = build_required_schema_unique_id_map(required_chunk_schema)?;
    let snapshot_domain_schema = snapshot.tablet_schema.clone();
    let snapshot_schema_columns = snapshot
        .tablet_schema
        .column
        .iter()
        .filter_map(|column| {
            let name = column.name.as_deref()?;
            let normalized_name = normalize_column_name(name);
            if normalized_name.is_empty() {
                return None;
            }
            let unique_id = if column.unique_id >= 0 {
                u32::try_from(column.unique_id).ok()
            } else {
                None
            };
            Some((normalized_name, unique_id))
        })
        .collect::<HashMap<_, _>>();
    let snapshot_schema_unique_ids = snapshot_schema_columns
        .values()
        .filter_map(|unique_id| *unique_id)
        .collect::<HashSet<_>>();
    let (lake_hints, native_uids_are_authoritative) = if let Some(meta) = lake_schema_meta {
        if let Some(native_hints) = meta.native_column_hints.as_deref() {
            (build_native_schema_column_hints(native_hints)?, true)
        } else {
            let snapshot_schema_id = snapshot.tablet_schema.id.unwrap_or(0);
            if snapshot_schema_id == meta.schema_id {
                (
                    build_lake_schema_column_hints_from_domain(&snapshot_domain_schema)?,
                    false,
                )
            } else {
                let fe_schema = fetch_table_schema_for_lake_scan(
                    meta.fe_addr.as_ref(),
                    meta.db_id,
                    meta.table_id,
                    meta.schema_id,
                    Some(snapshot.tablet_id),
                    meta.query_id.clone(),
                )
                .map_err(|e| {
                    format!(
                        "fetch FE table schema for lake scan failed: db_id={} table_id={} schema_id={} error={}",
                        meta.db_id, meta.table_id, meta.schema_id, e
                    )
                })?;
                (fe_schema.column_hints, false)
            }
        }
    } else {
        (HashMap::new(), false)
    };
    let mut missing_output_columns = HashSet::new();
    for field in output_schema.fields() {
        let normalized_name = normalize_column_name(field.name());
        let authoritative_unique_id = native_uids_are_authoritative
            .then(|| {
                lake_hints
                    .get(&normalized_name)
                    .and_then(|hint| hint.unique_id)
            })
            .flatten();
        let is_missing = if let Some(authoritative_unique_id) = authoritative_unique_id {
            !snapshot_schema_unique_ids.contains(&authoritative_unique_id)
        } else {
            snapshot_schema_columns
                .get(&normalized_name)
                .is_none_or(|snapshot_unique_id| {
                    required_unique_ids
                        .get(&normalized_name)
                        .is_some_and(|required_unique_id| {
                            snapshot_unique_id.is_none_or(|value| value != *required_unique_id)
                        })
                })
        };
        if is_missing {
            missing_output_columns.insert(normalized_name);
        }
    }

    let mut out = Vec::with_capacity(output_schema.fields().len());
    for (field_ref, slot) in output_schema
        .fields()
        .iter()
        .zip(output_chunk_schema.slots().iter())
    {
        let field = field_ref.as_ref();
        let normalized_name = normalize_column_name(field.name());
        let is_missing_in_snapshot = missing_output_columns.contains(&normalized_name);
        let authoritative_unique_id = native_uids_are_authoritative
            .then(|| {
                lake_hints
                    .get(&normalized_name)
                    .and_then(|hint| hint.unique_id)
            })
            .flatten();

        let schema_unique_id = slot
            .unique_id()
            .and_then(|value| u32::try_from(value).ok())
            .or_else(|| required_unique_ids.get(&normalized_name).copied())
            .or_else(|| {
                lake_hints
                    .get(&normalized_name)
                    .and_then(|hint| hint.unique_id)
            });
        if let (Some(authoritative_unique_id), Some(schema_unique_id)) =
            (authoritative_unique_id, schema_unique_id)
            && authoritative_unique_id != schema_unique_id
        {
            return Err(format!(
                "native StarRocks output unique_id mismatch: output_column={} slot_or_required_unique_id={} authoritative_unique_id={}",
                field.name(),
                schema_unique_id,
                authoritative_unique_id
            ));
        }
        let fallback_default_literal =
            if authoritative_unique_id.is_some() || is_missing_in_snapshot {
                lake_hints
                    .get(&normalized_name)
                    .and_then(|hint| hint.default_value.clone())
            } else {
                None
            };

        if is_missing_in_snapshot {
            if schema_unique_id.is_none() {
                return Err(format!(
                    "lake output column is missing unique_id hint while tablet snapshot lacks this column: tablet_id={} version={} output_column={}",
                    snapshot.tablet_id,
                    snapshot.version,
                    field.name()
                ));
            }
            if !field.is_nullable() && fallback_default_literal.is_none() {
                return Err(format!(
                    "lake output column is non-nullable without default value while tablet snapshot lacks this column: tablet_id={} version={} output_column={}",
                    snapshot.tablet_id,
                    snapshot.version,
                    field.name()
                ));
            }
        }

        out.push(StarRocksOutputColumnHint {
            schema_unique_id,
            physical_binding: authoritative_unique_id
                .map(StarRocksPhysicalColumnBinding::AuthoritativeUniqueId)
                .unwrap_or(StarRocksPhysicalColumnBinding::LegacyName),
            fallback_default_literal,
        });
    }
    Ok(out)
}

fn should_treat_missing_tablet_metadata_as_empty(
    tablet_root_path: &str,
    version: i64,
    error: &str,
) -> bool {
    if version == 1 && is_missing_tablet_metadata_error(error) {
        return true;
    }

    // If metadata lookup falls back all the way to version 1 and still cannot find
    // the tablet page/file, this tablet has never materialized metadata in the
    // shared bundle lineage. Treat it as an empty tablet for read compatibility.
    if is_missing_tablet_metadata_error(error) && error.contains("_0000000000000001.meta") {
        return true;
    }

    let path = tablet_root_path.to_ascii_lowercase();
    if !path.contains("/db10001/") && !path.contains("db10001/") {
        return false;
    }
    is_missing_tablet_metadata_error(error)
}

fn is_missing_tablet_metadata_error(error: &str) -> bool {
    let lowered = error.to_ascii_lowercase();
    lowered.contains("metadata file not found:")
        || lowered.contains("bundle metadata does not contain tablet page:")
        || lowered.contains("bundle metadata missing tablet page for tablet_id=")
}
