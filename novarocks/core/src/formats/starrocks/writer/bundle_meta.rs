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
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

use crate::connector::starrocks::lake::storage_domain::{StorageBundleFile, StorageTabletMetadata};
use crate::connector::starrocks::ports::StorageMetadataProvider;
use crate::connector::starrocks::schema::StarRocksTabletSchema;
use crate::formats::starrocks::fs_access::resolve_format_path;
use crate::formats::starrocks::writer::io::read_bytes_if_exists;
use crate::formats::starrocks::writer::io::write_bytes;
use crate::formats::starrocks::writer::layout::{
    BUNDLE_TABLET_ID, INITIAL_VERSION, META_DIR, bundle_meta_file_path, initial_meta_file_path,
    join_tablet_path, standalone_meta_file_path, tablet_meta_rel_path,
};
use futures::TryStreamExt;
use novarocks_fs::FsScheme;

static BUNDLE_META_WRITE_LOCKS: OnceLock<Mutex<HashMap<String, Arc<Mutex<()>>>>> = OnceLock::new();

fn bundle_meta_write_locks() -> &'static Mutex<HashMap<String, Arc<Mutex<()>>>> {
    BUNDLE_META_WRITE_LOCKS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Provider-backed bundle writes retain only core storage domain facts. The
/// compat provider owns the protobuf page encoding at the storage boundary.
pub struct StorageBundleMetaWriteEntry<'a> {
    pub tablet_id: i64,
    pub schema: &'a StarRocksTabletSchema,
    pub tablet_meta: &'a StorageTabletMetadata,
}

fn with_bundle_meta_write_lock<T>(
    tablet_root_path: &str,
    version: i64,
    f: impl FnOnce() -> Result<T, String>,
) -> Result<T, String> {
    if version <= 0 {
        return Err(format!(
            "invalid version for bundle metadata write lock: {}",
            version
        ));
    }
    let root = tablet_root_path.trim().trim_end_matches('/');
    if root.is_empty() {
        return Err("invalid tablet_root_path for bundle metadata write lock".to_string());
    }

    let key = format!("{root}:{version}");
    let lock = {
        let mut guard = bundle_meta_write_locks()
            .lock()
            .map_err(|_| "lock bundle meta write lock map failed".to_string())?;
        guard
            .entry(key)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    };
    let _lock_guard = lock
        .lock()
        .map_err(|_| "lock bundle meta write entry failed".to_string())?;
    f()
}

/// Writes a standalone tablet metadata page through the installed storage
/// boundary codec.  Compat owns the protobuf representation; core only hands
/// over the domain facts that belong in the page.
pub fn write_standalone_meta_file_with_provider(
    tablet_root_path: &str,
    tablet_id: i64,
    version: i64,
    tablet_meta: &StorageTabletMetadata,
    provider: &dyn StorageMetadataProvider,
) -> Result<(), String> {
    let meta_path = standalone_meta_file_path(tablet_root_path, tablet_id, version)?;
    let bytes = provider.encode_tablet_metadata(tablet_meta).map_err(|error| {
        format!(
            "encode standalone tablet metadata failed: tablet_id={tablet_id} version={version} error={error}"
        )
    })?;
    write_bytes(&meta_path, bytes)
}

/// Writes the initial raw tablet metadata page through the installed storage
/// boundary codec.  This preserves the StarRocks raw-v1 layout without making
/// the execution kernel depend on generated protobuf values.
pub fn write_initial_meta_file_with_provider(
    tablet_root_path: &str,
    tablet_meta: &StorageTabletMetadata,
    provider: &dyn StorageMetadataProvider,
) -> Result<(), String> {
    let meta_path = initial_meta_file_path(tablet_root_path)?;
    let bytes = provider
        .encode_tablet_metadata(tablet_meta)
        .map_err(|error| format!("encode initial tablet metadata failed: {error}"))?;
    write_bytes(&meta_path, bytes)
}

/// Reads the latest metadata page as protocol-neutral storage facts.  Compat
/// supplies the file codec, so this path never materializes generated protobuf
/// messages inside the storage kernel.
pub fn load_latest_tablet_metadata_with_provider(
    tablet_root_path: &str,
    tablet_id: i64,
    provider: &dyn StorageMetadataProvider,
) -> Result<(i64, StorageTabletMetadata), String> {
    let latest_version = discover_latest_tablet_metadata_version(tablet_root_path, tablet_id)?;
    let Some(latest_version) = latest_version else {
        return Ok((
            0,
            StorageTabletMetadata {
                id: Some(tablet_id),
                ..Default::default()
            },
        ));
    };
    if latest_version <= 0 {
        return Ok((
            0,
            StorageTabletMetadata {
                id: Some(tablet_id),
                ..Default::default()
            },
        ));
    }
    if let Some(metadata) = load_tablet_metadata_at_version_with_provider(
        tablet_root_path,
        tablet_id,
        latest_version,
        provider,
    )? {
        return Ok((latest_version, metadata));
    }
    Ok((
        0,
        StorageTabletMetadata {
            id: Some(tablet_id),
            ..Default::default()
        },
    ))
}

pub fn load_tablet_metadata_at_version_with_provider(
    tablet_root_path: &str,
    tablet_id: i64,
    version: i64,
    provider: &dyn StorageMetadataProvider,
) -> Result<Option<StorageTabletMetadata>, String> {
    if version <= 0 {
        return Ok(Some(StorageTabletMetadata {
            id: Some(tablet_id),
            ..Default::default()
        }));
    }
    for path in metadata_path_candidates(tablet_root_path, tablet_id, tablet_id, version)? {
        let Some(bytes) = read_bytes_if_exists(&path)? else {
            continue;
        };
        let metadata = provider.decode_tablet_metadata(&bytes).map_err(|error| {
            format!("decode standalone tablet metadata failed: path={path}, error={error}")
        })?;
        validate_storage_metadata_identity(&metadata, tablet_id, version, &path)?;
        return Ok(Some(metadata));
    }
    if version == INITIAL_VERSION {
        for path in metadata_path_candidates(
            tablet_root_path,
            tablet_id,
            BUNDLE_TABLET_ID,
            INITIAL_VERSION,
        )? {
            let Some(bytes) = read_bytes_if_exists(&path)? else {
                continue;
            };
            match provider.decode_tablet_metadata(&bytes) {
                Ok(mut metadata) if metadata.version == Some(INITIAL_VERSION) => {
                    metadata.id = Some(tablet_id);
                    return Ok(Some(metadata));
                }
                Ok(_) | Err(_) => {
                    let bundle = provider.decode_bundle_file(&bytes).map_err(|error| {
                        format!("decode initial metadata failed: path={path}, error={error}")
                    })?;
                    let page = bundle
                        .tablet_metadata_pages
                        .get(&tablet_id)
                        .ok_or_else(|| {
                            format!("bundle metadata missing tablet page for tablet_id={tablet_id}")
                        })?;
                    let metadata = provider.decode_tablet_metadata(page).map_err(|error| {
                        format!("decode initial bundle tablet metadata failed: path={path}, error={error}")
                    })?;
                    return Ok(Some(metadata));
                }
            }
        }
        return Ok(None);
    }
    let path = bundle_meta_file_path(tablet_root_path, version)?;
    let Some(bytes) = read_bytes_if_exists(&path)? else {
        return Ok(None);
    };
    let bundle = provider
        .decode_bundle_file(&bytes)
        .map_err(|error| format!("decode bundle metadata failed: path={path}, error={error}"))?;
    let page = bundle
        .tablet_metadata_pages
        .get(&tablet_id)
        .ok_or_else(|| format!("bundle metadata missing tablet page for tablet_id={tablet_id}"))?;
    let metadata = provider.decode_tablet_metadata(page).map_err(|error| {
        format!("decode bundle tablet metadata failed: path={path}, error={error}")
    })?;
    validate_storage_metadata_identity(&metadata, tablet_id, version, &path)?;
    Ok(Some(metadata))
}

/// Loads a bundle metadata file as protocol-neutral facts. The caller can
/// enumerate `tablet_metadata_pages` without materializing StarRocks protobuf
/// values in the storage kernel.
pub fn load_bundle_file_with_provider(
    tablet_root_path: &str,
    version: i64,
    provider: &dyn StorageMetadataProvider,
) -> Result<Option<StorageBundleFile>, String> {
    let path = bundle_meta_file_path(tablet_root_path, version)?;
    let Some(bytes) = read_bytes_if_exists(&path)? else {
        return Ok(None);
    };
    provider
        .decode_bundle_file(&bytes)
        .map(Some)
        .map_err(|error| format!("decode bundle metadata failed: path={path}, error={error}"))
}

/// Decodes a single bundle page through the explicitly installed storage wire
/// provider. Page lookup stays domain-only and retains the existing missing
/// tablet error text.
pub fn decode_bundle_tablet_metadata_with_provider(
    bundle: &StorageBundleFile,
    tablet_id: i64,
    provider: &dyn StorageMetadataProvider,
) -> Result<StorageTabletMetadata, String> {
    let page = bundle
        .tablet_metadata_pages
        .get(&tablet_id)
        .ok_or_else(|| format!("bundle metadata missing tablet page for tablet_id={tablet_id}"))?;
    provider.decode_tablet_metadata(page).map_err(|error| {
        format!("decode bundle tablet metadata failed: tablet_id={tablet_id}, error={error}")
    })
}

fn validate_storage_metadata_identity(
    metadata: &StorageTabletMetadata,
    tablet_id: i64,
    expected_version: i64,
    path: &str,
) -> Result<(), String> {
    if metadata.id != Some(tablet_id) {
        return Err(format!(
            "tablet metadata id mismatch: expected={tablet_id} actual={:?} path={path}",
            metadata.id
        ));
    }
    if metadata.version != Some(expected_version) {
        return Err(format!(
            "tablet metadata version mismatch: expected={expected_version} actual={:?} path={path}",
            metadata.version
        ));
    }
    Ok(())
}

fn metadata_path_candidates(
    tablet_root_path: &str,
    tablet_id: i64,
    metadata_file_tablet_id: i64,
    version: i64,
) -> Result<Vec<String>, String> {
    let rel = tablet_meta_rel_path(metadata_file_tablet_id, version)?;
    let mut paths = vec![join_tablet_path(tablet_root_path, &rel)?];
    paths.push(join_tablet_path(
        tablet_root_path,
        &format!("{tablet_id}/{rel}"),
    )?);
    paths.dedup();
    Ok(paths)
}

/// Compat-owned bundle-file framing path.  The core publisher supplies opaque
/// tablet pages and schema facts; the installed provider owns the StarRocks
/// bundle protobuf/footer encoding and page-version rewrite.
pub fn write_bundle_meta_file_with_provider(
    tablet_root_path: &str,
    tablet_id: i64,
    version: i64,
    schema: &StarRocksTabletSchema,
    tablet_meta: &StorageTabletMetadata,
    provider: &dyn StorageMetadataProvider,
) -> Result<(), String> {
    write_bundle_meta_file_batch_with_provider(
        tablet_root_path,
        version,
        &[StorageBundleMetaWriteEntry {
            tablet_id,
            schema,
            tablet_meta,
        }],
        provider,
    )
}

pub fn write_bundle_meta_file_batch_with_provider(
    tablet_root_path: &str,
    version: i64,
    entries: &[StorageBundleMetaWriteEntry<'_>],
    provider: &dyn StorageMetadataProvider,
) -> Result<(), String> {
    if entries.is_empty() {
        return Ok(());
    }
    with_bundle_meta_write_lock(tablet_root_path, version, || {
        let meta_path = bundle_meta_file_path(tablet_root_path, version)?;
        let mut tablet_metadata_pages = HashMap::new();
        let mut tablet_to_schema = HashMap::new();
        let mut schemas = HashMap::new();

        let mut merge_bundle = |bytes: &[u8], source: &str, bump_to_version: bool| {
            let bundle = provider.decode_bundle_file(bytes).map_err(|error| {
                format!("decode existing compat bundle failed: source={source} error={error}")
            })?;
            tablet_to_schema.extend(bundle.tablet_to_schema);
            schemas.extend(bundle.schemas);
            for (tablet_id, page) in bundle.tablet_metadata_pages {
                let page = if bump_to_version {
                    provider
                        .rewrite_tablet_metadata_version(&page, version)
                        .map_err(|error| {
                            format!(
                                "rewrite existing compat bundle tablet metadata failed: source={source} tablet_id={tablet_id} error={error}"
                            )
                        })?
                } else {
                    page
                };
                tablet_metadata_pages.insert(tablet_id, page);
            }
            Ok::<_, String>(())
        };

        if version > 1 {
            let previous_meta_path = bundle_meta_file_path(tablet_root_path, version - 1)?;
            if let Some(previous_bytes) = read_bytes_if_exists(&previous_meta_path)?
                && !(version - 1 == INITIAL_VERSION
                    && is_initial_raw_metadata_bytes_with_provider(&previous_bytes, provider)
                        .unwrap_or(false))
            {
                merge_bundle(&previous_bytes, &previous_meta_path, true)?;
            }
        }
        if let Some(existing_bytes) = read_bytes_if_exists(&meta_path)? {
            merge_bundle(&existing_bytes, &meta_path, false)?;
        }

        for entry in entries {
            let schema_id = entry
                .schema
                .id
                .filter(|value| *value > 0)
                .ok_or_else(|| "bundle schema id is missing".to_string())?;
            let encoded_metadata = provider.encode_tablet_metadata(entry.tablet_meta).map_err(
                |error| {
                    format!(
                        "encode compat bundle tablet metadata failed: tablet_id={} error={error}",
                        entry.tablet_id
                    )
                },
            )?;
            tablet_metadata_pages.insert(entry.tablet_id, encoded_metadata);
            tablet_to_schema.insert(entry.tablet_id, schema_id);
            schemas.insert(schema_id, entry.schema.clone());
        }
        let bytes = provider.encode_bundle_file(&StorageBundleFile {
            tablet_metadata_pages,
            tablet_to_schema,
            schemas,
        })?;
        write_bytes(&meta_path, bytes)
    })
}

fn is_initial_raw_metadata_bytes_with_provider(
    bytes: &[u8],
    provider: &dyn StorageMetadataProvider,
) -> Result<bool, String> {
    match provider.decode_tablet_metadata(bytes) {
        Ok(metadata) => Ok(metadata.version == Some(INITIAL_VERSION)),
        Err(_) => Ok(false),
    }
}

#[allow(dead_code)]
pub fn discover_latest_bundle_version(tablet_root_path: &str) -> Result<Option<i64>, String> {
    let meta_root = join_tablet_path(tablet_root_path, META_DIR)?;
    let mut latest: Option<i64> = None;
    for name in list_metadata_file_names(&meta_root)? {
        if let Some(version) = parse_bundle_version_from_meta_file_name(&name) {
            latest = Some(latest.map(|prev| prev.max(version)).unwrap_or(version));
        }
    }
    Ok(latest)
}

#[allow(dead_code)]
pub fn discover_latest_tablet_metadata_version(
    tablet_root_path: &str,
    tablet_id: i64,
) -> Result<Option<i64>, String> {
    discover_latest_tablet_metadata_version_at_most(tablet_root_path, tablet_id, i64::MAX)
}

#[allow(dead_code)]
pub fn discover_latest_tablet_metadata_version_at_most(
    tablet_root_path: &str,
    tablet_id: i64,
    max_version: i64,
) -> Result<Option<i64>, String> {
    if max_version <= 0 {
        return Ok(None);
    }
    let mut latest = discover_latest_metadata_version_in_dir(
        &join_tablet_path(tablet_root_path, META_DIR)?,
        tablet_id,
        max_version,
    )?;
    let tablet_scoped = discover_latest_metadata_version_in_dir(
        &join_tablet_path(tablet_root_path, &format!("{tablet_id}/{META_DIR}"))?,
        tablet_id,
        max_version,
    )?;
    if let Some(tablet_scoped) = tablet_scoped {
        latest = Some(
            latest
                .map(|v| v.max(tablet_scoped))
                .unwrap_or(tablet_scoped),
        );
    }
    Ok(latest)
}

fn discover_latest_metadata_version_in_dir(
    meta_root: &str,
    tablet_id: i64,
    max_version: i64,
) -> Result<Option<i64>, String> {
    let mut latest: Option<i64> = None;
    for name in list_metadata_file_names(meta_root)? {
        if let Some((file_tablet_id, version)) = parse_meta_file_name(&name)
            && version <= max_version
            && (file_tablet_id == tablet_id || file_tablet_id == BUNDLE_TABLET_ID)
        {
            latest = Some(latest.map(|prev| prev.max(version)).unwrap_or(version));
        }
    }
    Ok(latest)
}

fn list_metadata_file_names(meta_root: &str) -> Result<Vec<String>, String> {
    let access = resolve_format_path(meta_root)?;
    match access.scheme() {
        FsScheme::Local => list_metadata_file_names_local(meta_root),
        FsScheme::ObjectStore => list_metadata_file_names_object_store(meta_root, &access),
        FsScheme::Hdfs => Err(format!(
            "discover latest metadata version does not support hdfs path yet: {}",
            meta_root
        )),
    }
}

fn list_metadata_file_names_local(meta_root: &str) -> Result<Vec<String>, String> {
    let dir = PathBuf::from(meta_root);
    if !dir.exists() {
        return Ok(Vec::new());
    }
    if !dir.is_dir() {
        return Err(format!("meta path is not a directory: {}", meta_root));
    }

    let mut names = Vec::new();
    for entry in fs::read_dir(&dir).map_err(|e| {
        format!(
            "read local meta directory failed: path={}, error={}",
            meta_root, e
        )
    })? {
        let entry = entry.map_err(|e| {
            format!(
                "iterate local meta directory failed: path={}, error={}",
                meta_root, e
            )
        })?;
        let file_type = entry.file_type().map_err(|e| {
            format!(
                "inspect local meta directory entry failed: path={}, error={}",
                meta_root, e
            )
        })?;
        if file_type.is_dir() {
            continue;
        }
        if let Some(name) = entry.file_name().to_str() {
            names.push(name.to_string());
        }
    }
    Ok(names)
}

fn list_metadata_file_names_object_store(
    meta_root: &str,
    access: &crate::formats::starrocks::fs_access::StarRocksFormatPathAccess,
) -> Result<Vec<String>, String> {
    let rel_root = access.single_relative_path()?.to_string();
    let list_prefix = if rel_root.is_empty() {
        String::new()
    } else if rel_root.ends_with('/') {
        rel_root
    } else {
        format!("{}/", rel_root)
    };
    crate::fs::object_store::oss_block_on(async {
        let mut names = Vec::new();
        let mut lister = access
            .operator()
            .lister_with(&list_prefix)
            .recursive(false)
            .await
            .map_err(|e| {
                format!(
                    "list oss meta directory failed: path={}, error={}",
                    meta_root, e
                )
            })?;
        while let Some(entry) = lister.try_next().await.map_err(|e| {
            format!(
                "iterate oss meta directory failed: path={}, error={}",
                meta_root, e
            )
        })? {
            if entry.metadata().is_dir() {
                continue;
            }
            let path = entry.path();
            names.push(path.rsplit('/').next().unwrap_or(path).to_string());
        }
        Ok(names)
    })?
}

#[allow(dead_code)]
pub fn parse_bundle_version_from_meta_file_name(name: &str) -> Option<i64> {
    let (tablet_id, version) = parse_meta_file_name(name)?;
    (tablet_id == BUNDLE_TABLET_ID && version != INITIAL_VERSION).then_some(version)
}

fn parse_meta_file_name(name: &str) -> Option<(i64, i64)> {
    let trimmed = name.trim();
    if trimmed.is_empty() || !trimmed.to_ascii_lowercase().ends_with(".meta") {
        return None;
    }
    let stem = &trimmed[..trimmed.len().saturating_sub(5)];
    let mut parts = stem.split('_');
    let tablet_hex = parts.next()?;
    let version_hex = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    if tablet_hex.len() != 16 || version_hex.len() != 16 {
        return None;
    }
    let tablet_u64 = u64::from_str_radix(tablet_hex, 16).ok()?;
    let version_u64 = u64::from_str_radix(version_hex, 16).ok()?;
    if version_u64 == 0 || tablet_u64 > i64::MAX as u64 || version_u64 > i64::MAX as u64 {
        return None;
    }
    Some((tablet_u64 as i64, version_u64 as i64))
}
