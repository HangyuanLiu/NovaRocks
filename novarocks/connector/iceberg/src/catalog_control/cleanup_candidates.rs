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

//! Provider-private orphan candidate discovery.
//!
//! Discovery is planning-only. The caller must freeze every returned identity
//! in a durable artifact before dispatching a destructive operation.

use std::collections::{HashMap, HashSet};

use crate::commit::{FileSet, enumerate_files_for_snapshots, puffin_half_reference_protection};
use crate::fs_io;
use crate::iceberg::io::FileIO;
use crate::iceberg::spec::TableMetadata;
use crate::iceberg::table::Table;
use novarocks_fs::{FsLocation, FsScheme, ObjectStoreConfig};

#[derive(Clone, Debug)]
pub(super) struct ScannedFile {
    pub path: String,
    pub last_modified_ms: i64,
    pub size: Option<u64>,
    pub etag: Option<String>,
    pub version: Option<String>,
}

pub(super) async fn collect_orphan_candidates(
    table: &Table,
    older_than_ms: i64,
    object_store_config: Option<&ObjectStoreConfig>,
) -> Result<Vec<ScannedFile>, String> {
    let metadata = table.metadata();
    let file_io = table.file_io();
    let location = metadata.location().trim_end_matches('/');
    let snapshot_ids = metadata
        .snapshots()
        .map(|snapshot| snapshot.snapshot_id())
        .collect::<HashSet<_>>();
    let mut live: FileSet = enumerate_files_for_snapshots(file_io, metadata, &snapshot_ids)
        .await
        .map_err(|error| error.to_string())?;
    live.extend(
        metadata
            .metadata_log()
            .iter()
            .map(|entry| entry.metadata_file.clone()),
    );
    if let Some(current) = table.metadata_location() {
        live.insert(current.to_string());
    }
    live.insert(format!("{location}/metadata/version-hint.text"));
    live.extend(
        metadata
            .statistics_iter()
            .map(|statistics| statistics.statistics_path.clone()),
    );
    live.extend(
        metadata
            .partition_statistics_iter()
            .map(|statistics| statistics.statistics_path.clone()),
    );

    let canonical_root = canonical_containment(location);
    for child in [format!("{location}/data"), format!("{location}/metadata")] {
        if !canonical_containment(&child).starts_with(&canonical_root) {
            return Err(format!(
                "orphan cleanup scan path `{child}` escapes table location `{location}`"
            ));
        }
    }
    let scanned = list_files(location, object_store_config).await?;
    let mut candidate_paths = scanned
        .iter()
        .filter(|file| {
            let normalized = normalize_scanned_path(&file.path);
            !live.contains(&file.path) && !live.contains(&normalized)
        })
        .filter(|file| file.last_modified_ms < older_than_ms)
        .map(|file| file.path.clone())
        .collect::<FileSet>();
    let dv_index = build_dv_index(metadata, file_io, &snapshot_ids).await?;
    puffin_half_reference_protection(&mut candidate_paths, &dv_index, &live);
    let mut selected = scanned
        .into_iter()
        .filter(|file| candidate_paths.contains(&file.path))
        .collect::<Vec<_>>();
    selected.sort_by(|left, right| left.path.cmp(&right.path));
    if selected.windows(2).any(|pair| pair[0].path == pair[1].path) {
        return Err("orphan cleanup scan produced duplicate locations".to_string());
    }
    Ok(selected)
}

async fn list_files(
    location: &str,
    object_store_config: Option<&ObjectStoreConfig>,
) -> Result<Vec<ScannedFile>, String> {
    let parsed = FsLocation::parse(location)
        .map_err(|error| format!("parse orphan cleanup location `{location}`: {error}"))?;
    match parsed.scheme() {
        FsScheme::Local => {
            let prefix = if parsed.uri_scheme().is_some() {
                "file://"
            } else {
                ""
            };
            let root = std::path::PathBuf::from(parsed.path());
            let mut files = Vec::new();
            for child in [root.join("data"), root.join("metadata")] {
                if child.exists() {
                    walk_local(&child, prefix, &mut files)?;
                }
            }
            Ok(files)
        }
        FsScheme::ObjectStore | FsScheme::Hdfs => {
            list_opendal(parsed.original(), object_store_config).await
        }
    }
}

fn walk_local(
    directory: &std::path::Path,
    prefix: &str,
    files: &mut Vec<ScannedFile>,
) -> Result<(), String> {
    for entry in std::fs::read_dir(directory)
        .map_err(|error| format!("read orphan cleanup directory: {error}"))?
    {
        let entry = entry.map_err(|error| format!("read orphan cleanup entry: {error}"))?;
        let file_type = entry
            .file_type()
            .map_err(|error| format!("read orphan cleanup file type: {error}"))?;
        if file_type.is_dir() {
            walk_local(&entry.path(), prefix, files)?;
        } else if file_type.is_file() {
            let metadata = entry.metadata().ok();
            let mtime = metadata
                .as_ref()
                .and_then(|metadata| metadata.modified().ok())
                .and_then(|value| value.duration_since(std::time::UNIX_EPOCH).ok())
                .and_then(|value| i64::try_from(value.as_millis()).ok())
                .unwrap_or(i64::MAX);
            files.push(ScannedFile {
                path: format!("{prefix}{}", entry.path().to_string_lossy()),
                last_modified_ms: mtime,
                size: metadata.map(|metadata| metadata.len()),
                etag: None,
                version: None,
            });
        }
    }
    Ok(())
}

async fn list_opendal(
    location: &str,
    object_store_config: Option<&ObjectStoreConfig>,
) -> Result<Vec<ScannedFile>, String> {
    let access = fs_io::resolve_access_for_location(location, object_store_config)
        .map_err(|error| format!("resolve orphan cleanup location: {error}"))?;
    let operator = access.operator();
    let root = access
        .single_relative_path()
        .map_err(|error| format!("resolve orphan cleanup key: {error}"))?
        .trim_matches('/')
        .to_string();
    let mut files = Vec::new();
    for child in ["data", "metadata"] {
        let prefix = if root.is_empty() {
            format!("{child}/")
        } else {
            format!("{root}/{child}/")
        };
        let entries = match operator.list(&prefix).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == crate::opendal::ErrorKind::NotFound => continue,
            Err(error) => return Err(format!("list orphan cleanup prefix `{prefix}`: {error}")),
        };
        for entry in entries {
            if entry.metadata().is_dir() {
                continue;
            }
            files.push(ScannedFile {
                path: fs_io::format_resolved_location(access.handle(), entry.path())
                    .map_err(|error| format!("format orphan cleanup path: {error}"))?,
                last_modified_ms: entry
                    .metadata()
                    .last_modified()
                    .map(|value| canonical_object_mtime_ms(value.into_inner().as_millisecond()))
                    .unwrap_or(i64::MAX),
                size: Some(entry.metadata().content_length()),
                etag: entry.metadata().etag().map(ToOwned::to_owned),
                version: entry.metadata().version().map(ToOwned::to_owned),
            });
        }
    }
    Ok(files)
}

async fn build_dv_index(
    metadata: &TableMetadata,
    file_io: &FileIO,
    snapshot_ids: &HashSet<i64>,
) -> Result<HashMap<String, HashSet<String>>, String> {
    let mut index = HashMap::<String, HashSet<String>>::new();
    for snapshot_id in snapshot_ids {
        let Some(snapshot) = metadata.snapshot_by_id(*snapshot_id) else {
            continue;
        };
        let manifest_list = snapshot
            .load_manifest_list(file_io, metadata)
            .await
            .map_err(|error| format!("load orphan cleanup manifest list: {error}"))?;
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file
                .load_manifest(file_io)
                .await
                .map_err(|error| format!("load orphan cleanup manifest: {error}"))?;
            for entry in manifest.entries() {
                let file = entry.data_file();
                if let Some(referenced) = file.referenced_data_file() {
                    index
                        .entry(file.file_path().to_string())
                        .or_default()
                        .insert(referenced);
                }
            }
        }
    }
    Ok(index)
}

pub(super) fn canonical_object_mtime_ms(value: i64) -> i64 {
    value.div_euclid(1_000) * 1_000
}

fn normalize_scanned_path(path: &str) -> String {
    if path.contains("://") || !path.starts_with('/') {
        path.to_string()
    } else {
        format!("file://{path}")
    }
}

fn canonical_containment(location: &str) -> String {
    let stripped = location.strip_prefix("file://").unwrap_or(location);
    format!("{}/", stripped.trim_end_matches('/'))
}
