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
use crate::connector::starrocks::lake::schema_adapter::build_tablet_schema_pb_from_thrift;
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
use crate::service::grpc_client::proto::starrocks::{
    AggStateDescPb, ColumnPb, PTypeDesc, PTypeNode, TabletIndexPb, TabletSchemaPb,
};
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
    current_tablet_schema: &TabletSchemaPb,
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

    fn push_type_desc(&mut self, value: &PTypeDesc) {
        self.push_u64(value.types.len() as u64);
        for node in &value.types {
            self.push_type_node(node);
        }
    }

    fn push_type_node(&mut self, value: &PTypeNode) {
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

    fn push_agg_state(&mut self, value: &AggStateDescPb) {
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

    fn push_column(&mut self, value: &ColumnPb) {
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

    fn push_table_index(&mut self, value: &TabletIndexPb) {
        self.push_optional_i64(value.index_id);
        self.push_optional_string(value.index_name.as_deref());
        self.push_optional_i32(value.index_type);
        self.push_u64(value.col_unique_id.len() as u64);
        for unique_id in &value.col_unique_id {
            self.push_i32(*unique_id);
        }
        self.push_optional_string(value.index_properties.as_deref());
    }

    fn push_tablet_schema(&mut self, value: &TabletSchemaPb) {
        self.push_optional_i32(value.keys_type);
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

fn tablet_schema_semantic_fingerprint(schema: &TabletSchemaPb) -> String {
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
        None,
    )
    .map_err(|e| {
        format!(
            "fetch FE table schema for lake scan failed while refreshing snapshot schema: db_id={} table_id={} schema_id={} tablet_id={} error={}",
            meta.db_id, meta.table_id, meta.schema_id, snapshot.tablet_id, e
        )
    })?;
    let refreshed_schema = build_tablet_schema_pb_from_thrift(&fe_schema)?;
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
            &snapshot.tablet_schema,
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

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct LakeSchemaColumnHint {
    unique_id: Option<u32>,
    default_value: Option<String>,
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

fn build_lake_schema_column_hints(
    schema: &crate::thrift::agent_service::TTabletSchema,
) -> Result<HashMap<String, LakeSchemaColumnHint>, String> {
    let mut out = HashMap::new();
    for column in &schema.columns {
        let normalized_name = normalize_column_name(&column.column_name);
        if normalized_name.is_empty() {
            continue;
        }
        let unique_id = match column.col_unique_id {
            Some(v) if v >= 0 => Some(u32::try_from(v).map_err(|_| {
                format!(
                    "invalid FE table schema col_unique_id for column '{}': {}",
                    column.column_name, v
                )
            })?),
            _ => None,
        };
        let hint = LakeSchemaColumnHint {
            unique_id,
            default_value: column.default_value.clone(),
        };
        if let Some(existing) = out.get(&normalized_name)
            && existing != &hint
        {
            return Err(format!(
                "duplicated FE table schema column with mismatched metadata: column_name={}",
                column.column_name
            ));
        }
        out.insert(normalized_name, hint);
    }
    Ok(out)
}

fn build_lake_schema_column_hints_from_pb(
    schema: &crate::service::grpc_client::proto::starrocks::TabletSchemaPb,
) -> Result<HashMap<String, LakeSchemaColumnHint>, String> {
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
        let hint = LakeSchemaColumnHint {
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
) -> Result<HashMap<String, LakeSchemaColumnHint>, String> {
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
        let hint = LakeSchemaColumnHint {
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
                    build_lake_schema_column_hints_from_pb(&snapshot.tablet_schema)?,
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
                    None,
                )
                .map_err(|e| {
                    format!(
                        "fetch FE table schema for lake scan failed: db_id={} table_id={} schema_id={} error={}",
                        meta.db_id, meta.table_id, meta.schema_id, e
                    )
                })?;
                (build_lake_schema_column_hints(&fe_schema)?, false)
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

#[cfg(test)]
mod tests {
    use super::{
        StarRocksNativeReader, StarRocksOutputColumnHint, StarRocksPhysicalColumnBinding,
        build_output_column_hints, build_required_schema_unique_id_map,
        maybe_refresh_snapshot_schema_for_lake_scan, schema_signature_with_hints,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;

    use crate::cache::{DataCacheManager, DataCachePageCacheOptions};
    use crate::common::ids::SlotId;
    use crate::connector::starrocks::{LakeScanSchemaMeta, StarRocksSchemaColumnHint};
    use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
    use crate::formats::starrocks::metadata::{StarRocksSegmentFile, StarRocksTabletSnapshot};
    use crate::formats::starrocks::plan::{
        StarRocksTableModelPlan, build_native_read_plan_with_output_hints,
    };
    use crate::formats::starrocks::reader::build_native_record_batch;
    use crate::formats::starrocks::segment::decode_segment_footer;
    use crate::formats::starrocks::writer::bundle_meta::write_standalone_meta_file;
    use crate::formats::starrocks::writer::{
        build_starrocks_native_segment_bytes, write_parquet_file,
    };
    use crate::service::grpc_client::proto::starrocks::{
        ColumnPb, KeysType, RowsetMetadataPb, TabletMetadataPb, TabletSchemaPb,
    };
    use arrow::array::Int64Array;
    use arrow::record_batch::RecordBatch;

    fn current_chunk_schema() -> Arc<ChunkSchema> {
        Arc::new(
            ChunkSchema::try_new(vec![
                ChunkSlotSchema::new_with_field(
                    SlotId::new(1),
                    Field::new("id", DataType::Int64, false),
                    None,
                    Some(0),
                ),
                ChunkSlotSchema::new_with_field(
                    SlotId::new(2),
                    Field::new("flag", DataType::Boolean, false),
                    None,
                    Some(12),
                ),
            ])
            .expect("current chunk schema"),
        )
    }

    fn open_real_empty_parquet_reader(arrow_nullable: bool) -> Result<RecordBatch, String> {
        let tablet_id = 42_001;
        let version = 7;
        let temp_dir = tempfile::tempdir().expect("create empty parquet reader temp dir");
        let parquet_path = temp_dir.path().join("data/empty.parquet");
        let parquet_schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::Int64,
            arrow_nullable,
        )]));
        write_parquet_file(
            parquet_path.to_str().expect("UTF-8 parquet path"),
            &RecordBatch::new_empty(parquet_schema),
        )
        .expect("write real empty parquet file");

        let tablet_schema = TabletSchemaPb {
            id: Some(30),
            keys_type: Some(KeysType::DupKeys as i32),
            column: vec![ColumnPb {
                unique_id: 11,
                name: Some("v".to_string()),
                r#type: "BIGINT".to_string(),
                is_nullable: Some(false),
                visible: Some(true),
                ..Default::default()
            }],
            ..Default::default()
        };
        let metadata = TabletMetadataPb {
            id: Some(tablet_id),
            version: Some(version),
            schema: Some(tablet_schema),
            rowsets: vec![RowsetMetadataPb {
                id: Some(1),
                segments: vec!["empty.parquet".to_string()],
                num_rows: Some(0),
                ..Default::default()
            }],
            ..Default::default()
        };
        write_standalone_meta_file(
            temp_dir.path().to_str().expect("UTF-8 tablet root"),
            tablet_id,
            version,
            &metadata,
        )
        .expect("write standalone tablet metadata");

        let output_chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                Field::new("v", DataType::Int64, false),
                None,
                Some(11),
            )])
            .expect("empty parquet output chunk schema"),
        );
        let output_schema = output_chunk_schema.arrow_schema_ref();
        let mut reader = StarRocksNativeReader::open(
            tablet_id,
            temp_dir.path().to_str().expect("UTF-8 tablet root"),
            version,
            output_chunk_schema.clone(),
            output_chunk_schema,
            HashMap::new(),
            Vec::new(),
            None,
            None,
        )?;
        reader
            .get_next(&output_schema)?
            .ok_or_else(|| "empty parquet reader returned no batch".to_string())
    }

    #[test]
    fn handled_empty_parquet_reader_returns_empty_batch_without_segment_fallthrough() {
        let batch = open_real_empty_parquet_reader(false)
            .expect("identified empty parquet must be handled by the parquet reader");

        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
        assert!(!batch.schema().field(0).is_nullable());
    }

    #[test]
    fn handled_empty_parquet_reader_validates_schema_before_returning_empty() {
        let err = open_real_empty_parquet_reader(true)
            .expect_err("empty parquet schema drift must fail before returning an empty batch");

        assert!(
            err.contains("physical parquet schema nullability does not match Arrow field")
                && err.contains("physical_nullable=false")
                && err.contains("arrow_nullable=true"),
            "{err}"
        );
    }

    fn old_snapshot() -> StarRocksTabletSnapshot {
        StarRocksTabletSnapshot {
            tablet_id: 300,
            version: 7,
            metadata_path: "/tmp/300_7.meta".to_string(),
            tablet_schema: TabletSchemaPb {
                id: Some(29),
                column: vec![ColumnPb {
                    unique_id: 0,
                    name: Some("id".to_string()),
                    r#type: "BIGINT".to_string(),
                    is_nullable: Some(false),
                    ..Default::default()
                }],
                ..Default::default()
            },
            historical_schemas: BTreeMap::new(),
            total_num_rows: 1,
            rowset_count: 1,
            segment_files: Vec::new(),
            delete_predicates: Vec::new(),
            delvec_meta: Default::default(),
        }
    }

    fn native_schema_meta() -> LakeScanSchemaMeta {
        LakeScanSchemaMeta {
            db_id: 10,
            table_id: 20,
            schema_id: 30,
            fe_addr: None,
            query_id: None,
            native_tablet_schema: Some(TabletSchemaPb {
                id: Some(30),
                keys_type: Some(KeysType::DupKeys as i32),
                column: vec![
                    ColumnPb {
                        unique_id: 0,
                        name: Some("id".to_string()),
                        r#type: "BIGINT".to_string(),
                        is_key: Some(true),
                        is_nullable: Some(false),
                        visible: Some(true),
                        ..Default::default()
                    },
                    ColumnPb {
                        unique_id: 12,
                        name: Some("flag".to_string()),
                        r#type: "BOOLEAN".to_string(),
                        is_key: Some(false),
                        is_nullable: Some(false),
                        default_value: Some(b"false".to_vec()),
                        visible: Some(true),
                        ..Default::default()
                    },
                ],
                num_short_key_columns: Some(1),
                sort_key_idxes: vec![0],
                sort_key_unique_ids: vec![0],
                ..Default::default()
            }),
            native_column_hints: Some(vec![
                StarRocksSchemaColumnHint {
                    name: "id".to_string(),
                    unique_id: 0,
                    default_value: None,
                },
                StarRocksSchemaColumnHint {
                    name: "flag".to_string(),
                    unique_id: 12,
                    default_value: Some("false".to_string()),
                },
            ]),
        }
    }

    #[test]
    fn schema_signature_distinguishes_slot_metadata() {
        let schema_a = Arc::new(Schema::new(vec![Field::new("v2", DataType::Utf8, false)]));
        let schema_b = Arc::new(Schema::new(vec![Field::new("v2", DataType::Utf8, false)]));
        let chunk_schema_a = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(2),
                Field::new("v2", DataType::Utf8, false),
                None,
                None,
            )])
            .expect("chunk schema a"),
        );
        let chunk_schema_b = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(4),
                Field::new("v2", DataType::Utf8, false),
                None,
                None,
            )])
            .expect("chunk schema b"),
        );
        let hints = vec![StarRocksOutputColumnHint {
            schema_unique_id: None,
            physical_binding: StarRocksPhysicalColumnBinding::LegacyName,
            fallback_default_literal: None,
        }];
        let current_schema = TabletSchemaPb::default();
        let sig_a =
            schema_signature_with_hints(&schema_a, &chunk_schema_a, &hints, &current_schema)
                .expect("signature a");
        let sig_b =
            schema_signature_with_hints(&schema_b, &chunk_schema_b, &hints, &current_schema)
                .expect("signature b");
        assert_ne!(
            sig_a, sig_b,
            "slot metadata must be part of cache signature"
        );
    }

    fn cache_signature_schema(
        keys_type: KeysType,
        value_type: &str,
        aggregation: Option<&str>,
    ) -> TabletSchemaPb {
        TabletSchemaPb {
            id: Some(30),
            keys_type: Some(keys_type as i32),
            column: vec![
                ColumnPb {
                    unique_id: 1,
                    name: Some("k".to_string()),
                    r#type: "BIGINT".to_string(),
                    is_key: Some(true),
                    is_nullable: Some(false),
                    visible: Some(false),
                    ..Default::default()
                },
                ColumnPb {
                    unique_id: 2,
                    name: Some("v".to_string()),
                    r#type: value_type.to_string(),
                    is_key: Some(false),
                    aggregation: aggregation.map(str::to_string),
                    is_nullable: Some(false),
                    default_value: Some(b"7".to_vec()),
                    visible: Some(true),
                    children_columns: vec![ColumnPb {
                        unique_id: 0,
                        name: Some("nested".to_string()),
                        r#type: "INT".to_string(),
                        is_key: Some(false),
                        is_nullable: Some(true),
                        visible: Some(true),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
            num_short_key_columns: Some(1),
            sort_key_idxes: vec![0],
            sort_key_unique_ids: vec![1],
            ..Default::default()
        }
    }

    fn cache_signature_output() -> (
        Arc<Schema>,
        Arc<ChunkSchema>,
        Vec<StarRocksOutputColumnHint>,
    ) {
        let output_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let output_chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(2),
                Field::new("v", DataType::Int64, false),
                None,
                Some(2),
            )])
            .expect("cache signature output chunk schema"),
        );
        let hints = vec![StarRocksOutputColumnHint {
            schema_unique_id: Some(2),
            physical_binding: StarRocksPhysicalColumnBinding::AuthoritativeUniqueId(2),
            fallback_default_literal: Some("7".to_string()),
        }];
        (output_schema, output_chunk_schema, hints)
    }

    #[test]
    fn native_batch_cache_signature_separates_same_id_dup_and_agg_semantics() {
        let _ = DataCacheManager::instance().init_page_cache(DataCachePageCacheOptions {
            capacity: 64,
            evict_probability: 100,
        });
        let dup_schema = cache_signature_schema(KeysType::DupKeys, "BIGINT", None);
        let agg_schema = cache_signature_schema(KeysType::AggKeys, "BIGINT", Some("SUM"));
        assert_eq!(dup_schema.id, agg_schema.id);
        assert_ne!(dup_schema, agg_schema);
        let (output_schema, output_chunk_schema, hints) = cache_signature_output();
        let dup_signature =
            schema_signature_with_hints(&output_schema, &output_chunk_schema, &hints, &dup_schema)
                .expect("DUP cache signature");
        let agg_signature =
            schema_signature_with_hints(&output_schema, &output_chunk_schema, &hints, &agg_schema)
                .expect("AGG cache signature");
        let batch = RecordBatch::try_new(
            output_schema,
            vec![Arc::new(Int64Array::from(vec![10_i64, 20_i64]))],
        )
        .expect("cached DUP batch");
        let cache_path = "final4-cache-signature-dup-agg";
        crate::formats::starrocks::cache::native_batch_cache_put(
            cache_path,
            10,
            20,
            &dup_signature,
            batch,
        );

        assert!(
            crate::formats::starrocks::cache::native_batch_cache_get(
                cache_path,
                10,
                20,
                &agg_signature,
            )
            .is_none(),
            "same-ID AGG semantics must not hit a DUP batch cache entry; dup_schema={dup_schema:?} agg_schema={agg_schema:?}"
        );
    }

    #[test]
    fn native_batch_cache_signature_separates_same_id_recursive_column_semantics() {
        let current_a = cache_signature_schema(KeysType::DupKeys, "BIGINT", None);
        let mut current_b = current_a.clone();
        current_b.column[1].r#type = "VARCHAR".to_string();
        current_b.column[1].is_nullable = Some(true);
        current_b.column[1].default_value = Some(b"changed".to_vec());
        current_b.column[1].visible = Some(false);
        current_b.column[1].children_columns[0].r#type = "BIGINT".to_string();
        assert_eq!(current_a.id, current_b.id);
        assert_ne!(current_a, current_b);
        let (output_schema, output_chunk_schema, hints) = cache_signature_output();
        let signature_a =
            schema_signature_with_hints(&output_schema, &output_chunk_schema, &hints, &current_a)
                .expect("first recursive schema cache signature");
        let signature_b =
            schema_signature_with_hints(&output_schema, &output_chunk_schema, &hints, &current_b)
                .expect("second recursive schema cache signature");

        assert_ne!(
            signature_a, signature_b,
            "same-ID recursive current schema drift must change the cache signature; current_a={current_a:?} current_b={current_b:?}"
        );
    }

    #[test]
    fn required_schema_preserves_zero_unique_id() {
        let schema = current_chunk_schema();
        let unique_ids = build_required_schema_unique_id_map(&schema)
            .expect("zero is a valid StarRocks storage unique id");

        assert_eq!(unique_ids.get("id"), Some(&0));
    }

    #[test]
    fn native_schema_replaces_stale_snapshot_without_fe_refresh() {
        let snapshot = old_snapshot();
        let meta = native_schema_meta();

        let refreshed = maybe_refresh_snapshot_schema_for_lake_scan(&snapshot, Some(&meta))
            .expect("complete native schema must avoid FE schema refresh");
        assert_eq!(
            refreshed.tablet_schema,
            meta.native_tablet_schema.clone().unwrap()
        );

        let chunk_schema = current_chunk_schema();
        let hints = build_output_column_hints(
            &refreshed,
            &chunk_schema,
            &chunk_schema.arrow_schema_ref(),
            &chunk_schema,
            Some(&meta),
        )
        .expect("native hints must cover current columns missing from old snapshot");
        assert_eq!(hints[0].schema_unique_id, Some(0));
        assert_eq!(hints[1].schema_unique_id, Some(12));
        assert_eq!(
            hints[1].physical_binding,
            StarRocksPhysicalColumnBinding::AuthoritativeUniqueId(12)
        );
        assert_eq!(hints[1].fallback_default_literal.as_deref(), Some("false"));
    }

    #[test]
    fn native_schema_mismatch_rejects_incomplete_native_metadata() {
        let snapshot = old_snapshot();
        let mut meta = native_schema_meta();
        meta.native_tablet_schema = None;

        let err = maybe_refresh_snapshot_schema_for_lake_scan(&snapshot, Some(&meta))
            .expect_err("native schema-id mismatch must require the full current schema");
        assert!(err.contains("full current schema"), "{err}");
    }

    #[test]
    fn native_schema_replaces_same_id_snapshot_with_semantic_drift() {
        let mut snapshot = old_snapshot();
        snapshot.tablet_schema.id = Some(30);
        let meta = native_schema_meta();

        let refreshed = maybe_refresh_snapshot_schema_for_lake_scan(&snapshot, Some(&meta))
            .expect("authoritative native schema must replace same-id stale semantics");
        assert_eq!(
            refreshed.tablet_schema,
            meta.native_tablet_schema.expect("native schema")
        );
    }

    #[test]
    fn native_current_schema_drives_hidden_key_model_and_physical_type() {
        let temp_dir = tempfile::tempdir().expect("create native schema temp dir");
        let current_schema = TabletSchemaPb {
            id: Some(30),
            keys_type: Some(KeysType::AggKeys as i32),
            column: vec![
                ColumnPb {
                    unique_id: 1,
                    name: Some("k".to_string()),
                    r#type: "BIGINT".to_string(),
                    is_key: Some(true),
                    is_nullable: Some(false),
                    visible: Some(false),
                    ..Default::default()
                },
                ColumnPb {
                    unique_id: 2,
                    name: Some("v".to_string()),
                    r#type: "BIGINT".to_string(),
                    is_key: Some(false),
                    aggregation: Some("SUM".to_string()),
                    is_nullable: Some(false),
                    visible: Some(true),
                    ..Default::default()
                },
            ],
            num_short_key_columns: Some(1),
            sort_key_idxes: vec![0],
            sort_key_unique_ids: vec![1],
            ..Default::default()
        };
        let segment_schema = Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let input = RecordBatch::try_new(
            segment_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 2])),
                Arc::new(Int64Array::from(vec![10, 20, 5])),
            ],
        )
        .expect("build schema-30 segment batch");
        let segment_bytes = build_starrocks_native_segment_bytes(&input, &current_schema)
            .expect("encode schema-30 native segment");
        let segment_name = "schema-30-hidden-key.dat";
        let segment_path = temp_dir.path().join(segment_name);
        std::fs::write(&segment_path, &segment_bytes).expect("write native segment");
        let footer = decode_segment_footer(segment_name, &segment_bytes)
            .expect("decode schema-30 segment footer");

        let stale_schema = TabletSchemaPb {
            id: Some(29),
            keys_type: Some(KeysType::DupKeys as i32),
            column: vec![
                ColumnPb {
                    unique_id: 1,
                    name: Some("k".to_string()),
                    r#type: "INT".to_string(),
                    is_key: Some(false),
                    is_nullable: Some(false),
                    visible: Some(false),
                    ..Default::default()
                },
                ColumnPb {
                    unique_id: 2,
                    name: Some("v".to_string()),
                    r#type: "BIGINT".to_string(),
                    is_key: Some(false),
                    aggregation: None,
                    is_nullable: Some(false),
                    visible: Some(true),
                    ..Default::default()
                },
            ],
            num_short_key_columns: Some(0),
            ..Default::default()
        };
        let snapshot = StarRocksTabletSnapshot {
            tablet_id: 10,
            version: 20,
            metadata_path: temp_dir.path().join("meta").display().to_string(),
            tablet_schema: stale_schema,
            historical_schemas: std::collections::BTreeMap::from([(30, current_schema.clone())]),
            total_num_rows: 3,
            rowset_count: 1,
            segment_files: vec![StarRocksSegmentFile {
                name: segment_name.to_string(),
                relative_path: format!(
                    "{}/{}",
                    temp_dir
                        .path()
                        .file_name()
                        .expect("temp dir name")
                        .to_string_lossy(),
                    segment_name
                ),
                path: segment_path.display().to_string(),
                rowset_version: 1,
                schema_id: Some(30),
                segment_id: Some(0),
                bundle_file_offset: Some(0),
                segment_size: Some(segment_bytes.len() as u64),
            }],
            delete_predicates: vec![],
            delvec_meta: Default::default(),
        };
        let meta = LakeScanSchemaMeta {
            db_id: 1,
            table_id: 2,
            schema_id: 30,
            fe_addr: None,
            query_id: None,
            native_tablet_schema: Some(current_schema),
            native_column_hints: Some(vec![StarRocksSchemaColumnHint {
                name: "v".to_string(),
                unique_id: 2,
                default_value: None,
            }]),
        };
        let refreshed = maybe_refresh_snapshot_schema_for_lake_scan(&snapshot, Some(&meta))
            .expect("install authoritative current schema");
        let output_chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(2),
                Field::new("v", DataType::Int64, false),
                None,
                Some(2),
            )])
            .expect("output chunk schema"),
        );
        let output_schema = output_chunk_schema.arrow_schema_ref();
        let hints = build_output_column_hints(
            &refreshed,
            &output_chunk_schema,
            &output_schema,
            &output_chunk_schema,
            Some(&meta),
        )
        .expect("build authoritative output hints");
        let plan = build_native_read_plan_with_output_hints(
            &refreshed,
            std::slice::from_ref(&footer),
            &output_schema,
            &hints,
            None,
        )
        .expect("plan AGG_KEYS read with hidden key");
        assert_eq!(plan.table_model, StarRocksTableModelPlan::AggKeys);
        assert_eq!(plan.group_key_columns[0].schema_type, "BIGINT");
        assert_eq!(
            plan.projected_columns[0].schema.aggregation.as_deref(),
            Some("SUM")
        );

        let result = build_native_record_batch(
            &plan,
            &[footer],
            temp_dir.path().to_str().expect("UTF-8 temp path"),
            None,
            &output_schema,
            &[],
        )
        .expect("read and aggregate schema-30 segment");
        let values = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("BIGINT aggregate values");
        assert_eq!(values.values(), &[30, 5]);
    }

    #[test]
    fn native_schema_hints_preserve_current_default_when_snapshot_has_unique_id() {
        let mut snapshot = old_snapshot();
        snapshot.tablet_schema.column.push(ColumnPb {
            unique_id: 12,
            name: Some("flag".to_string()),
            r#type: "BOOLEAN".to_string(),
            is_nullable: Some(false),
            default_value: Some(b"true".to_vec()),
            ..Default::default()
        });
        let meta = native_schema_meta();
        let chunk_schema = current_chunk_schema();

        let hints = build_output_column_hints(
            &snapshot,
            &chunk_schema,
            &chunk_schema.arrow_schema_ref(),
            &chunk_schema,
            Some(&meta),
        )
        .expect("native hints must preserve the current schema default provenance");

        assert_eq!(
            hints[1].physical_binding,
            StarRocksPhysicalColumnBinding::AuthoritativeUniqueId(12)
        );
        assert_eq!(hints[1].fallback_default_literal.as_deref(), Some("false"));
    }

    #[test]
    fn native_schema_hints_do_not_mark_renamed_same_uid_column_missing() {
        let mut snapshot = old_snapshot();
        snapshot.tablet_schema.column.push(ColumnPb {
            unique_id: 11,
            name: Some("old_flag".to_string()),
            r#type: "BOOLEAN".to_string(),
            is_nullable: Some(false),
            ..Default::default()
        });
        let chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(2),
                Field::new("new_flag", DataType::Boolean, false),
                None,
                Some(11),
            )])
            .expect("renamed current chunk schema"),
        );
        let meta = LakeScanSchemaMeta {
            native_column_hints: Some(vec![StarRocksSchemaColumnHint {
                name: "new_flag".to_string(),
                unique_id: 11,
                default_value: None,
            }]),
            ..native_schema_meta()
        };

        let hints = build_output_column_hints(
            &snapshot,
            &chunk_schema,
            &chunk_schema.arrow_schema_ref(),
            &chunk_schema,
            Some(&meta),
        )
        .expect("renamed column must be present through its authoritative unique id");

        assert_eq!(hints[0].schema_unique_id, Some(11));
        assert_eq!(
            hints[0].physical_binding,
            StarRocksPhysicalColumnBinding::AuthoritativeUniqueId(11)
        );
        assert_eq!(hints[0].fallback_default_literal, None);
    }

    #[test]
    fn slot_unique_id_without_native_provenance_keeps_legacy_name_binding() {
        let snapshot = old_snapshot();
        let chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                Field::new("id", DataType::Int64, false),
                None,
                Some(0),
            )])
            .expect("legacy chunk schema"),
        );

        let hints = build_output_column_hints(
            &snapshot,
            &chunk_schema,
            &chunk_schema.arrow_schema_ref(),
            &chunk_schema,
            None,
        )
        .expect("legacy slot hints");

        assert_eq!(hints[0].schema_unique_id, Some(0));
        assert_eq!(
            hints[0].physical_binding,
            StarRocksPhysicalColumnBinding::LegacyName
        );
    }
}
