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
//! StarRocks tablet metadata loader for native scan.
//!
//! This module resolves tablet snapshot metadata, validates layout assumptions,
//! and loads segment footers for bundle-backed segment files.
//!
//! Current limitations:
//! - Segment reads rely on local filesystem or S3-compatible object storage.

use std::collections::BTreeMap;

use opendal::{ErrorKind, Operator};
use prost::Message;

use crate::connector::starrocks::ObjectStoreProfile;
use crate::formats::starrocks::cache::{segment_footer_cache_get, segment_footer_cache_put};
use crate::formats::starrocks::fs_access::{
    StarRocksFormatTabletAccess, operator_relative_path_for_tablet_root,
    resolve_format_tablet_access, resolve_format_tablet_access_with_object_store_config,
};
use crate::formats::starrocks::range_read::{ensure_exact_range_read_len, expected_range_len};
use crate::formats::starrocks::segment::{StarRocksSegmentFooter, decode_segment_footer};
use crate::runtime::global_async_runtime::data_runtime;
use crate::service::grpc_client::proto::starrocks::{
    BundleTabletMetadataPb, DelvecPagePb, KeysType, PagePointerPb, RowsetMetadataPb,
    TabletMetadataPb, TabletSchemaPb,
};

const METADATA_DIR: &str = "meta";
const DATA_DIR: &str = "data";
const BUNDLE_METADATA_FOOTER_SIZE: usize = 8;
const INITIAL_VERSION: i64 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
/// One segment entry resolved from tablet rowsets.
pub struct StarRocksSegmentFile {
    pub name: String,
    pub relative_path: String,
    pub path: String,
    pub rowset_version: i64,
    pub schema_id: Option<i64>,
    pub segment_id: Option<u32>,
    pub bundle_file_offset: Option<i64>,
    pub segment_size: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// One delvec page pointer resolved from tablet metadata.
pub struct StarRocksDelvecPageRaw {
    pub version: i64,
    pub offset: u64,
    pub size: u64,
    pub crc32c: Option<u32>,
    pub crc32c_gen_version: Option<i64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
/// Minimal primary-key delvec metadata required by native reader.
pub struct StarRocksDelvecMetaRaw {
    pub version_to_file_rel_path: BTreeMap<i64, String>,
    pub segment_delvec_pages: BTreeMap<u32, StarRocksDelvecPageRaw>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// Minimal IN predicate shape extracted from StarRocks metadata.
pub struct StarRocksInPredicateRaw {
    pub column_name: String,
    pub is_not_in: bool,
    pub values: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// Minimal binary predicate shape extracted from StarRocks metadata.
pub struct StarRocksBinaryPredicateRaw {
    pub column_name: String,
    pub op: String,
    pub value: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// Minimal is-null predicate shape extracted from StarRocks metadata.
pub struct StarRocksIsNullPredicateRaw {
    pub column_name: String,
    pub is_not_null: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// One delete-predicate group extracted from rowset metadata.
pub struct StarRocksDeletePredicateRaw {
    pub version: i64,
    pub sub_predicates: Vec<String>,
    pub in_predicates: Vec<StarRocksInPredicateRaw>,
    pub binary_predicates: Vec<StarRocksBinaryPredicateRaw>,
    pub is_null_predicates: Vec<StarRocksIsNullPredicateRaw>,
}

#[derive(Clone, Debug, Default, PartialEq)]
/// Minimal metadata snapshot required for native read planning.
pub struct StarRocksTabletSnapshot {
    pub tablet_id: i64,
    pub version: i64,
    pub metadata_path: String,
    pub tablet_schema: TabletSchemaPb,
    pub historical_schemas: BTreeMap<i64, TabletSchemaPb>,
    pub total_num_rows: u64,
    pub rowset_count: usize,
    pub segment_files: Vec<StarRocksSegmentFile>,
    pub delete_predicates: Vec<StarRocksDeletePredicateRaw>,
    pub delvec_meta: StarRocksDelvecMetaRaw,
}

/// Load tablet metadata and build a minimal snapshot.
/// It supports both standalone metadata and bundle metadata layouts.
pub fn load_tablet_snapshot(
    tablet_id: i64,
    version: i64,
    tablet_root_path: &str,
    object_store_profile: Option<&ObjectStoreProfile>,
) -> Result<StarRocksTabletSnapshot, String> {
    if tablet_id <= 0 {
        return Err(format!(
            "invalid tablet_id for metadata loader: {tablet_id}"
        ));
    }
    if version <= 0 {
        return Err(format!(
            "invalid tablet version for metadata loader: {version}"
        ));
    }

    let access = resolve_format_tablet_access(tablet_root_path, object_store_profile)?;
    let rt = data_runtime()?;
    load_tablet_snapshot_from_root(tablet_id, version, &access, rt.as_ref())
}

pub fn load_tablet_snapshot_with_object_store_config(
    tablet_id: i64,
    version: i64,
    tablet_root_path: &str,
    object_store_config: Option<&crate::fs::object_store::ObjectStoreConfig>,
) -> Result<StarRocksTabletSnapshot, String> {
    if tablet_id <= 0 {
        return Err(format!(
            "invalid tablet_id for metadata loader: {tablet_id}"
        ));
    }
    if version <= 0 {
        return Err(format!(
            "invalid tablet version for metadata loader: {version}"
        ));
    }

    let access = resolve_format_tablet_access_with_object_store_config(
        tablet_root_path,
        object_store_config,
    )?;
    let rt = data_runtime()?;
    load_tablet_snapshot_from_root(tablet_id, version, &access, rt.as_ref())
}

fn load_tablet_snapshot_from_root(
    tablet_id: i64,
    version: i64,
    access: &StarRocksFormatTabletAccess,
    rt: &tokio::runtime::Runtime,
) -> Result<StarRocksTabletSnapshot, String> {
    load_tablet_snapshot_at_version(tablet_id, version, access, rt)
}

fn load_tablet_snapshot_at_version(
    tablet_id: i64,
    version: i64,
    access: &StarRocksFormatTabletAccess,
    rt: &tokio::runtime::Runtime,
) -> Result<StarRocksTabletSnapshot, String> {
    let op = access.operator();
    let standalone_metadata_rel = metadata_rel_path(tablet_id, version)?;
    for candidate_rel in metadata_rel_path_candidates(tablet_id, &standalone_metadata_rel) {
        let candidate_operator_rel = access.operator_relative_path(&candidate_rel);
        if object_exists(rt, &op, &candidate_operator_rel)? {
            let metadata_bytes = read_all_bytes(rt, &op, &candidate_operator_rel)?;
            let metadata_path = access.join_relative_path(&candidate_rel);
            return parse_standalone_snapshot(
                tablet_id,
                version,
                access,
                &metadata_path,
                &metadata_bytes,
            );
        }
    }

    if version == INITIAL_VERSION && tablet_id != 0 {
        let initial_metadata_rel = metadata_rel_path(0, INITIAL_VERSION)?;
        for candidate_rel in metadata_rel_path_candidates(tablet_id, &initial_metadata_rel) {
            let candidate_operator_rel = access.operator_relative_path(&candidate_rel);
            if object_exists(rt, &op, &candidate_operator_rel)? {
                let metadata_bytes = read_all_bytes(rt, &op, &candidate_operator_rel)?;
                let metadata_path = access.join_relative_path(&candidate_rel);
                return parse_initial_snapshot(
                    tablet_id,
                    version,
                    access,
                    &metadata_path,
                    &metadata_bytes,
                );
            }
        }
    }

    let bundle_metadata_rel = metadata_rel_path(0, version)?;
    for candidate_rel in metadata_rel_path_candidates(tablet_id, &bundle_metadata_rel) {
        let candidate_operator_rel = access.operator_relative_path(&candidate_rel);
        if object_exists(rt, &op, &candidate_operator_rel)? {
            let metadata_bytes = read_all_bytes(rt, &op, &candidate_operator_rel)?;
            let metadata_path = access.join_relative_path(&candidate_rel);
            return parse_bundle_snapshot(
                tablet_id,
                version,
                access,
                &metadata_path,
                &metadata_bytes,
            );
        }
    }
    Err(format!("metadata file not found: {}", bundle_metadata_rel))
}

/// Load and validate segment footers from a metadata snapshot.
/// Supports both bundle-backed ranges and standalone segment files.
pub fn load_bundle_segment_footers(
    snapshot: &StarRocksTabletSnapshot,
    tablet_root_path: &str,
    object_store_profile: Option<&ObjectStoreProfile>,
) -> Result<Vec<StarRocksSegmentFooter>, String> {
    if let Some(footers) =
        segment_footer_cache_get(tablet_root_path, snapshot.tablet_id, snapshot.version)
    {
        return Ok(footers);
    }

    let access = resolve_format_tablet_access(tablet_root_path, object_store_profile)?;
    let op = access.operator();
    let rt = data_runtime()?;

    let mut footers = Vec::with_capacity(snapshot.segment_files.len());
    for segment in &snapshot.segment_files {
        let segment_bytes = if let Some(bundle_offset) = segment.bundle_file_offset {
            let segment_size = segment.segment_size.ok_or_else(|| {
                format!(
                    "missing segment_size for bundle segment in metadata: path={}",
                    segment.path
                )
            })?;
            let start = u64::try_from(bundle_offset).map_err(|_| {
                format!(
                    "invalid bundle file offset for segment: path={}, offset={}",
                    segment.path, bundle_offset
                )
            })?;
            let end = start.checked_add(segment_size).ok_or_else(|| {
                format!(
                    "segment range overflow: path={}, offset={}, segment_size={}",
                    segment.path, bundle_offset, segment_size
                )
            })?;
            read_segment_bytes_for_footer(rt.as_ref(), &op, segment, start, end)?
        } else {
            read_all_bytes(rt.as_ref(), &op, &segment.relative_path)?
        };
        footers.push(decode_segment_footer(&segment.path, &segment_bytes)?);
    }

    segment_footer_cache_put(
        tablet_root_path,
        snapshot.tablet_id,
        snapshot.version,
        footers.clone(),
    );

    Ok(footers)
}

fn read_segment_bytes_for_footer(
    rt: &tokio::runtime::Runtime,
    op: &Operator,
    segment: &StarRocksSegmentFile,
    start: u64,
    end: u64,
) -> Result<Vec<u8>, String> {
    match read_range_bytes(rt, op, &segment.relative_path, start, end) {
        Ok(bytes) => Ok(bytes),
        Err(range_err)
            if start == 0
                && (range_err.contains("too little data")
                    || range_err.contains("unexpected length")) =>
        {
            read_all_bytes(rt, op, &segment.relative_path)
        }
        Err(range_err) => Err(range_err),
    }
}

pub(crate) fn tablet_operator_relative_path(
    tablet_root_path: &str,
    rel_path: &str,
) -> Result<String, String> {
    operator_relative_path_for_tablet_root(tablet_root_path, rel_path)
}

fn parse_standalone_snapshot(
    tablet_id: i64,
    version: i64,
    access: &StarRocksFormatTabletAccess,
    metadata_path: &str,
    metadata_bytes: &[u8],
) -> Result<StarRocksTabletSnapshot, String> {
    let metadata = TabletMetadataPb::decode(metadata_bytes).map_err(|e| {
        format!(
            "decode standalone TabletMetadataPB failed: path={}, error={}",
            metadata_path, e
        )
    })?;
    build_standalone_snapshot(tablet_id, version, access, metadata_path, metadata)
}

fn parse_initial_snapshot(
    tablet_id: i64,
    version: i64,
    access: &StarRocksFormatTabletAccess,
    metadata_path: &str,
    metadata_bytes: &[u8],
) -> Result<StarRocksTabletSnapshot, String> {
    let mut metadata = TabletMetadataPb::decode(metadata_bytes).map_err(|e| {
        format!(
            "decode initial TabletMetadataPB failed: path={}, error={}",
            metadata_path, e
        )
    })?;
    metadata.id = Some(tablet_id);
    build_standalone_snapshot(tablet_id, version, access, metadata_path, metadata)
}

fn build_standalone_snapshot(
    tablet_id: i64,
    version: i64,
    access: &StarRocksFormatTabletAccess,
    metadata_path: &str,
    metadata: TabletMetadataPb,
) -> Result<StarRocksTabletSnapshot, String> {
    ensure_tablet_identity(&metadata, tablet_id, version, metadata_path)?;
    let tablet_schema = metadata.schema.clone().ok_or_else(|| {
        format!(
            "tablet schema missing in standalone metadata: tablet_id={}, path={}",
            tablet_id, metadata_path
        )
    })?;
    let schema_id = tablet_schema.id.unwrap_or(-1);
    ensure_supported_keys_type(
        &tablet_schema,
        tablet_id,
        schema_id,
        "tablet",
        metadata_path,
    )?;
    for (rowset_schema_id, rowset_schema) in &metadata.historical_schemas {
        ensure_supported_keys_type(
            rowset_schema,
            tablet_id,
            *rowset_schema_id,
            "rowset",
            metadata_path,
        )?;
    }
    let (segment_files, delete_predicates) = collect_segment_files(access, &metadata)?;
    let total_num_rows = collect_total_num_rows(&metadata, tablet_id, metadata_path)?;
    let delvec_meta = collect_delvec_meta(access, &metadata)?;
    let mut historical_schemas = metadata
        .historical_schemas
        .clone()
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    if let Some(schema_id) = tablet_schema.id {
        historical_schemas
            .entry(schema_id)
            .or_insert_with(|| tablet_schema.clone());
    }

    Ok(StarRocksTabletSnapshot {
        tablet_id,
        version,
        metadata_path: metadata_path.to_string(),
        tablet_schema,
        historical_schemas,
        total_num_rows,
        rowset_count: metadata.rowsets.len(),
        segment_files,
        delete_predicates,
        delvec_meta,
    })
}

fn parse_bundle_snapshot(
    tablet_id: i64,
    version: i64,
    access: &StarRocksFormatTabletAccess,
    metadata_path: &str,
    metadata_bytes: &[u8],
) -> Result<StarRocksTabletSnapshot, String> {
    let (bundle, _bundle_meta_size) = decode_bundle_metadata(metadata_path, metadata_bytes)?;
    let page = bundle.tablet_meta_pages.get(&tablet_id).ok_or_else(|| {
        format!(
            "bundle metadata does not contain tablet page: tablet_id={}, path={}",
            tablet_id, metadata_path
        )
    })?;
    let mut metadata = decode_tablet_metadata_page(metadata_path, metadata_bytes, page)?;
    ensure_tablet_identity(&metadata, tablet_id, version, metadata_path)?;
    hydrate_schema_from_bundle(tablet_id, &bundle, &mut metadata, metadata_path)?;
    let tablet_schema = metadata.schema.clone().ok_or_else(|| {
        format!(
            "tablet schema missing after bundle schema hydration: tablet_id={}, path={}",
            tablet_id, metadata_path
        )
    })?;
    let (segment_files, delete_predicates) = collect_segment_files(access, &metadata)?;
    let total_num_rows = collect_total_num_rows(&metadata, tablet_id, metadata_path)?;
    let delvec_meta = collect_delvec_meta(access, &metadata)?;
    let mut historical_schemas = metadata
        .historical_schemas
        .clone()
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    if let Some(schema_id) = tablet_schema.id {
        historical_schemas
            .entry(schema_id)
            .or_insert_with(|| tablet_schema.clone());
    }

    Ok(StarRocksTabletSnapshot {
        tablet_id,
        version,
        metadata_path: metadata_path.to_string(),
        tablet_schema,
        historical_schemas,
        total_num_rows,
        rowset_count: metadata.rowsets.len(),
        segment_files,
        delete_predicates,
        delvec_meta,
    })
}

fn decode_bundle_metadata(
    metadata_path: &str,
    bytes: &[u8],
) -> Result<(BundleTabletMetadataPb, usize), String> {
    if bytes.len() < BUNDLE_METADATA_FOOTER_SIZE {
        return Err(format!(
            "invalid bundle metadata file: {} (file too small, size={})",
            metadata_path,
            bytes.len()
        ));
    }

    let footer_offset = bytes.len() - BUNDLE_METADATA_FOOTER_SIZE;
    let bundle_meta_size = u64::from_le_bytes(
        bytes[footer_offset..]
            .try_into()
            .map_err(|_| "decode bundle footer failed".to_string())?,
    ) as usize;
    if bundle_meta_size == 0 || bundle_meta_size > footer_offset {
        return Err(format!(
            "invalid bundle metadata footer: path={}, file_size={}, bundle_meta_size={}",
            metadata_path,
            bytes.len(),
            bundle_meta_size
        ));
    }

    let bundle_offset = footer_offset - bundle_meta_size;
    let bundle =
        BundleTabletMetadataPb::decode(&bytes[bundle_offset..footer_offset]).map_err(|e| {
            format!(
                "decode BundleTabletMetadataPB failed: path={}, error={}",
                metadata_path, e
            )
        })?;
    Ok((bundle, bundle_meta_size))
}

fn decode_tablet_metadata_page(
    metadata_path: &str,
    file_bytes: &[u8],
    page: &PagePointerPb,
) -> Result<TabletMetadataPb, String> {
    let offset = usize::try_from(page.offset).map_err(|_| {
        format!(
            "invalid tablet metadata page offset: path={}, offset={}",
            metadata_path, page.offset
        )
    })?;
    let size = usize::try_from(page.size).map_err(|_| {
        format!(
            "invalid tablet metadata page size: path={}, size={}",
            metadata_path, page.size
        )
    })?;
    let end = offset.saturating_add(size);
    if offset > file_bytes.len() || end > file_bytes.len() {
        return Err(format!(
            "tablet metadata page out of range: path={}, offset={}, size={}, file_size={}",
            metadata_path,
            offset,
            size,
            file_bytes.len()
        ));
    }

    TabletMetadataPb::decode(&file_bytes[offset..end]).map_err(|e| {
        format!(
            "decode TabletMetadataPB failed: path={}, offset={}, size={}, error={}",
            metadata_path, offset, size, e
        )
    })
}

fn ensure_tablet_identity(
    metadata: &TabletMetadataPb,
    tablet_id: i64,
    version: i64,
    metadata_path: &str,
) -> Result<(), String> {
    if metadata.id != Some(tablet_id) {
        return Err(format!(
            "tablet id mismatch in metadata page: expected={}, actual={:?}, path={}",
            tablet_id, metadata.id, metadata_path
        ));
    }
    if metadata.version != Some(version) {
        return Err(format!(
            "tablet version mismatch in metadata page: expected={}, actual={:?}, path={}",
            version, metadata.version, metadata_path
        ));
    }
    Ok(())
}

fn hydrate_schema_from_bundle(
    tablet_id: i64,
    bundle: &BundleTabletMetadataPb,
    metadata: &mut TabletMetadataPb,
    metadata_path: &str,
) -> Result<(), String> {
    let schema_id = bundle.tablet_to_schema.get(&tablet_id).ok_or_else(|| {
        format!(
            "tablet schema id missing in bundle metadata: tablet_id={}, path={}",
            tablet_id, metadata_path
        )
    })?;
    let schema = bundle.schemas.get(schema_id).ok_or_else(|| {
        format!(
            "tablet schema not found in bundle metadata: tablet_id={}, schema_id={}, path={}",
            tablet_id, schema_id, metadata_path
        )
    })?;
    ensure_supported_keys_type(schema, tablet_id, *schema_id, "tablet", metadata_path)?;

    metadata.schema = Some(schema.clone());
    metadata
        .historical_schemas
        .insert(*schema_id, schema.clone());

    for rowset_schema_id in metadata.rowset_to_schema.values() {
        let rowset_schema = bundle.schemas.get(rowset_schema_id).ok_or_else(|| {
            format!(
                "rowset schema not found in bundle metadata: tablet_id={}, schema_id={}, path={}",
                tablet_id, rowset_schema_id, metadata_path
            )
        })?;
        ensure_supported_keys_type(
            rowset_schema,
            tablet_id,
            *rowset_schema_id,
            "rowset",
            metadata_path,
        )?;
        metadata
            .historical_schemas
            .insert(*rowset_schema_id, rowset_schema.clone());
    }
    Ok(())
}

fn ensure_supported_keys_type(
    schema: &TabletSchemaPb,
    tablet_id: i64,
    schema_id: i64,
    schema_kind: &str,
    metadata_path: &str,
) -> Result<(), String> {
    let raw_keys_type = schema.keys_type.ok_or_else(|| {
        format!(
            "missing keys_type in tablet schema: tablet_id={}, schema_id={}, schema_kind={}, path={}",
            tablet_id, schema_id, schema_kind, metadata_path
        )
    })?;
    let keys_type = KeysType::try_from(raw_keys_type).map_err(|_| {
        format!(
            "unknown keys_type in tablet schema: tablet_id={}, schema_id={}, schema_kind={}, keys_type={}, path={}",
            tablet_id, schema_id, schema_kind, raw_keys_type, metadata_path
        )
    })?;
    if !matches!(
        keys_type,
        KeysType::DupKeys | KeysType::AggKeys | KeysType::UniqueKeys | KeysType::PrimaryKeys
    ) {
        return Err(format!(
            "unsupported keys_type for rust native starrocks reader: tablet_id={}, schema_id={}, schema_kind={}, keys_type={}, supported=[DUP_KEYS,AGG_KEYS,UNIQUE_KEYS,PRIMARY_KEYS], path={}",
            tablet_id,
            schema_id,
            schema_kind,
            keys_type.as_str_name(),
            metadata_path
        ));
    }
    Ok(())
}

fn collect_total_num_rows(
    metadata: &TabletMetadataPb,
    tablet_id: i64,
    metadata_path: &str,
) -> Result<u64, String> {
    let mut total_rows = 0_u64;
    for (idx, rowset) in metadata.rowsets.iter().enumerate() {
        let num_rows = rowset.num_rows.ok_or_else(|| {
            format!(
                "rowset num_rows is missing in tablet metadata: tablet_id={}, rowset_index={}, path={}",
                tablet_id, idx, metadata_path
            )
        })?;
        if num_rows < 0 {
            return Err(format!(
                "rowset num_rows is negative in tablet metadata: tablet_id={}, rowset_index={}, num_rows={}, path={}",
                tablet_id, idx, num_rows, metadata_path
            ));
        }
        total_rows = total_rows
            .checked_add(num_rows as u64)
            .ok_or_else(|| {
                format!(
                    "rowset num_rows overflow in tablet metadata: tablet_id={}, rowset_index={}, path={}",
                    tablet_id, idx, metadata_path
                )
            })?;
    }
    Ok(total_rows)
}

fn collect_segment_files(
    access: &StarRocksFormatTabletAccess,
    metadata: &TabletMetadataPb,
) -> Result<(Vec<StarRocksSegmentFile>, Vec<StarRocksDeletePredicateRaw>), String> {
    let mut files = Vec::new();
    let delete_predicates = collect_delete_predicates(metadata)?;
    for (rowset_index, rowset) in metadata.rowsets.iter().enumerate() {
        let rowset_version = lake_rowset_visibility_version(rowset, rowset_index)?;

        if !rowset.segment_size.is_empty() && rowset.segment_size.len() != rowset.segments.len() {
            return Err(format!(
                "invalid rowset segment_size: segments={}, segment_size={}",
                rowset.segments.len(),
                rowset.segment_size.len()
            ));
        }
        if !rowset.bundle_file_offsets.is_empty()
            && rowset.bundle_file_offsets.len() != rowset.segments.len()
        {
            return Err(format!(
                "invalid rowset bundle_file_offsets: segments={}, bundle_file_offsets={}",
                rowset.segments.len(),
                rowset.bundle_file_offsets.len()
            ));
        }
        if !rowset.bundle_file_offsets.is_empty()
            && rowset.segment_size.len() != rowset.segments.len()
        {
            return Err(format!(
                "bundle rowset missing segment_size: segments={}, segment_size={}",
                rowset.segments.len(),
                rowset.segment_size.len()
            ));
        }
        let rowset_schema_id = rowset
            .id
            .and_then(|rowset_id| metadata.rowset_to_schema.get(&rowset_id).copied())
            .or_else(|| metadata.schema.as_ref().and_then(|schema| schema.id));
        for (index, raw_name) in rowset.segments.iter().enumerate() {
            let name = raw_name.trim().trim_start_matches('/').to_string();
            if name.is_empty() {
                return Err("empty segment file name in rowset metadata".to_string());
            }
            let segment_id = match rowset.id {
                Some(rowset_id) => {
                    let ordinal = u32::try_from(index).map_err(|_| {
                        format!(
                            "segment index overflow while deriving segment_id: rowset_id={}, index={}",
                            rowset_id, index
                        )
                    })?;
                    Some(rowset_id.checked_add(ordinal).ok_or_else(|| {
                        format!(
                            "segment_id overflow while deriving rowset_id+segment_index: rowset_id={}, index={}",
                            rowset_id, index
                        )
                    })?)
                }
                None => None,
            };
            let bundle_file_offset = rowset.bundle_file_offsets.get(index).copied();
            if let Some(offset) = bundle_file_offset
                && offset < 0
            {
                return Err(format!(
                    "invalid negative bundle_file_offset in rowset metadata: segment={}, offset={}",
                    name, offset
                ));
            }
            let segment_size = rowset
                .segment_size
                .get(index)
                .copied()
                .filter(|size| *size > 0);
            if bundle_file_offset.is_some() && segment_size.is_none() {
                return Err(format!(
                    "missing segment_size for bundle segment in rowset metadata: segment={}",
                    name
                ));
            }
            let rel_path = format!("{DATA_DIR}/{name}");
            files.push(StarRocksSegmentFile {
                name,
                relative_path: access.operator_relative_path(&rel_path),
                path: access.join_relative_path(&rel_path),
                rowset_version,
                schema_id: rowset_schema_id,
                segment_id,
                bundle_file_offset,
                segment_size,
            });
        }
    }
    Ok((files, delete_predicates))
}

pub(crate) fn lake_rowset_visibility_version(
    rowset: &RowsetMetadataPb,
    rowset_index: usize,
) -> Result<i64, String> {
    // Keep lake-tablet semantics in StarRocks BE:
    // - delete predicate version key is rowset index, not delete_predicate.version.
    // - rowset scan applies delete predicates with version >= current rowset index.
    let rowset_version = i64::try_from(rowset_index).map_err(|_| {
        format!(
            "rowset index overflow while deriving delete visibility version: rowset_id={:?}, rowset_index={}",
            rowset.id, rowset_index
        )
    })?;
    if rowset_version < 0 {
        return Err(format!(
            "invalid rowset version in tablet metadata: rowset_id={:?}, version={}",
            rowset.id, rowset_version
        ));
    }
    Ok(rowset_version)
}

pub(crate) fn collect_delete_predicates(
    metadata: &TabletMetadataPb,
) -> Result<Vec<StarRocksDeletePredicateRaw>, String> {
    let mut delete_predicates = Vec::new();
    for (rowset_index, rowset) in metadata.rowsets.iter().enumerate() {
        let rowset_version = lake_rowset_visibility_version(rowset, rowset_index)?;
        let Some(delete_predicate) = rowset.delete_predicate.as_ref() else {
            continue;
        };
        delete_predicates.push(StarRocksDeletePredicateRaw {
            version: rowset_version,
            // Lake reader in StarRocks BE does not use sub_predicates.
            sub_predicates: Vec::new(),
            in_predicates: delete_predicate
                .in_predicates
                .iter()
                .map(|p| {
                    let column_name = p
                        .column_name
                        .as_deref()
                        .map(str::trim)
                        .filter(|v| !v.is_empty())
                        .ok_or_else(|| {
                            format!(
                                "delete in predicate column_name is missing: rowset_id={:?}, version={}",
                                rowset.id, rowset_version
                            )
                        })?;
                    Ok(StarRocksInPredicateRaw {
                        column_name: column_name.to_string(),
                        is_not_in: p.is_not_in.unwrap_or(false),
                        values: p.values.clone(),
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
            binary_predicates: delete_predicate
                .binary_predicates
                .iter()
                .map(|p| {
                    let column_name = p
                        .column_name
                        .as_deref()
                        .map(str::trim)
                        .filter(|v| !v.is_empty())
                        .ok_or_else(|| {
                            format!(
                                "delete binary predicate column_name is missing: rowset_id={:?}, version={}",
                                rowset.id, rowset_version
                            )
                        })?;
                    let op = p
                        .op
                        .as_deref()
                        .map(str::trim)
                        .filter(|v| !v.is_empty())
                        .ok_or_else(|| {
                            format!(
                                "delete binary predicate op is missing: rowset_id={:?}, version={}, column_name={}",
                                rowset.id, rowset_version, column_name
                            )
                        })?;
                    let value = p
                        .value
                        .as_deref()
                        .map(str::trim)
                        .filter(|v| !v.is_empty())
                        .ok_or_else(|| {
                            format!(
                                "delete binary predicate value is missing: rowset_id={:?}, version={}, column_name={}, op={}",
                                rowset.id, rowset_version, column_name, op
                            )
                        })?;
                    Ok(StarRocksBinaryPredicateRaw {
                        column_name: column_name.to_string(),
                        op: op.to_string(),
                        value: value.to_string(),
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
            is_null_predicates: delete_predicate
                .is_null_predicates
                .iter()
                .map(|p| {
                    let column_name = p
                        .column_name
                        .as_deref()
                        .map(str::trim)
                        .filter(|v| !v.is_empty())
                        .ok_or_else(|| {
                            format!(
                                "delete is-null predicate column_name is missing: rowset_id={:?}, version={}",
                                rowset.id, rowset_version
                            )
                        })?;
                    Ok(StarRocksIsNullPredicateRaw {
                        column_name: column_name.to_string(),
                        is_not_null: p.is_not_null.unwrap_or(false),
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
        });
    }
    Ok(delete_predicates)
}

fn collect_delvec_meta(
    access: &StarRocksFormatTabletAccess,
    metadata: &TabletMetadataPb,
) -> Result<StarRocksDelvecMetaRaw, String> {
    let mut out = StarRocksDelvecMetaRaw::default();
    let Some(raw) = metadata.delvec_meta.as_ref() else {
        return Ok(out);
    };

    for (version, file) in &raw.version_to_file {
        if *version < 0 {
            return Err(format!(
                "invalid delvec file version in metadata: {version}"
            ));
        }
        let name = file
            .name
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| format!("delvec file name is missing for version {}", version))?;
        let rel_path = format!("{DATA_DIR}/{}", name.trim_start_matches('/'));
        let rel = access.operator_relative_path(&rel_path);
        if out
            .version_to_file_rel_path
            .insert(*version, rel.clone())
            .is_some()
        {
            return Err(format!(
                "duplicated delvec file version in metadata: version={}, path={}",
                version, rel
            ));
        }
    }

    for (segment_id, page) in &raw.delvecs {
        let parsed = parse_delvec_page(*segment_id, page)?;
        if out
            .segment_delvec_pages
            .insert(*segment_id, parsed.clone())
            .is_some()
        {
            return Err(format!(
                "duplicated delvec page for segment_id in metadata: segment_id={}",
                segment_id
            ));
        }
    }

    Ok(out)
}

fn parse_delvec_page(
    segment_id: u32,
    page: &DelvecPagePb,
) -> Result<StarRocksDelvecPageRaw, String> {
    let version = page
        .version
        .ok_or_else(|| format!("missing delvec page version for segment_id {}", segment_id))?;
    if version < 0 {
        return Err(format!(
            "invalid delvec page version for segment_id {}: {}",
            segment_id, version
        ));
    }
    let offset = page
        .offset
        .ok_or_else(|| format!("missing delvec page offset for segment_id {}", segment_id))?;
    let size = page
        .size
        .ok_or_else(|| format!("missing delvec page size for segment_id {}", segment_id))?;
    Ok(StarRocksDelvecPageRaw {
        version,
        offset,
        size,
        crc32c: page.crc32c,
        crc32c_gen_version: page.crc32c_gen_version,
    })
}

fn metadata_rel_path(tablet_id: i64, version: i64) -> Result<String, String> {
    if tablet_id < 0 {
        return Err(format!(
            "invalid tablet id for metadata file name: {tablet_id}"
        ));
    }
    if version <= 0 {
        return Err(format!(
            "invalid tablet version for metadata file name: {version}"
        ));
    }
    let tablet_id_u64 = u64::try_from(tablet_id)
        .map_err(|_| format!("convert tablet_id to u64 failed: {tablet_id}"))?;
    let version_u64 =
        u64::try_from(version).map_err(|_| format!("convert version to u64 failed: {version}"))?;
    Ok(format!(
        "{METADATA_DIR}/{tablet_id_u64:016X}_{version_u64:016X}.meta"
    ))
}

fn metadata_rel_path_candidates(tablet_id: i64, rel_path: &str) -> Vec<String> {
    let normalized = rel_path.trim_start_matches('/').to_string();
    if normalized.is_empty() {
        return vec![normalized];
    }
    let tablet_prefix = format!("{tablet_id}/");
    if normalized.starts_with(&tablet_prefix) {
        vec![normalized]
    } else {
        vec![normalized.clone(), format!("{tablet_id}/{normalized}")]
    }
}

fn object_exists(rt: &tokio::runtime::Runtime, op: &Operator, path: &str) -> Result<bool, String> {
    const MAX_STAT_ATTEMPTS: usize = 4;
    for attempt in 1..=MAX_STAT_ATTEMPTS {
        match rt.block_on(op.stat(path)) {
            Ok(_) => return Ok(true),
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(false),
            Err(e) if e.is_temporary() && attempt < MAX_STAT_ATTEMPTS => {
                let backoff_ms = (100_u64).saturating_mul(1_u64 << (attempt - 1)).min(2_000);
                std::thread::sleep(std::time::Duration::from_millis(backoff_ms));
            }
            Err(e) => return Err(format!("stat object failed: path={}, error={}", path, e)),
        }
    }
    Err(format!("stat object failed after retries: path={}", path))
}

fn read_all_bytes(
    rt: &tokio::runtime::Runtime,
    op: &Operator,
    path: &str,
) -> Result<Vec<u8>, String> {
    const MAX_READ_ATTEMPTS: usize = 4;
    for attempt in 1..=MAX_READ_ATTEMPTS {
        match rt.block_on(op.read(path)) {
            Ok(v) => return Ok(v.to_vec()),
            Err(e) if e.kind() == ErrorKind::NotFound => {
                return Err(format!("metadata file not found: {}", path));
            }
            Err(e) if e.is_temporary() && attempt < MAX_READ_ATTEMPTS => {
                let backoff_ms = (100_u64).saturating_mul(1_u64 << (attempt - 1)).min(2_000);
                std::thread::sleep(std::time::Duration::from_millis(backoff_ms));
            }
            Err(e) => {
                return Err(format!(
                    "read metadata file failed: path={}, error={}",
                    path, e
                ));
            }
        }
    }
    Err(format!(
        "read metadata file failed after retries: path={}",
        path
    ))
}

fn read_range_bytes(
    rt: &tokio::runtime::Runtime,
    op: &Operator,
    path: &str,
    start: u64,
    end: u64,
) -> Result<Vec<u8>, String> {
    if end <= start {
        return Err(format!(
            "invalid read range for segment file: path={}, start={}, end={}",
            path, start, end
        ));
    }
    let expected_len = expected_range_len(path, start, end)?;
    const MAX_READ_ATTEMPTS: usize = 4;
    for attempt in 1..=MAX_READ_ATTEMPTS {
        match rt.block_on(op.read_with(path).range(start..end).into_future()) {
            Ok(v) => {
                let bytes = v.to_vec();
                match ensure_exact_range_read_len(path, start, end, bytes.len()) {
                    Ok(()) => return Ok(bytes),
                    Err(err) if attempt < MAX_READ_ATTEMPTS => {
                        let backoff_ms =
                            (100_u64).saturating_mul(1_u64 << (attempt - 1)).min(2_000);
                        std::thread::sleep(std::time::Duration::from_millis(backoff_ms));
                        continue;
                    }
                    Err(err) => {
                        return Err(format!(
                            "read segment file range failed: {err}, expected_bytes={expected_len}"
                        ));
                    }
                }
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {
                return Err(format!("segment file not found: {}", path));
            }
            Err(e) if e.is_temporary() && attempt < MAX_READ_ATTEMPTS => {
                let backoff_ms = (100_u64).saturating_mul(1_u64 << (attempt - 1)).min(2_000);
                std::thread::sleep(std::time::Duration::from_millis(backoff_ms));
            }
            Err(e) => {
                return Err(format!(
                    "read segment file range failed: path={}, range={}..{}, error={}",
                    path, start, end, e
                ));
            }
        }
    }
    Err(format!(
        "read segment file range failed after retries: path={}, range={}..{}",
        path, start, end
    ))
}
