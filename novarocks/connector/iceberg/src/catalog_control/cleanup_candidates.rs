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

use futures::TryStreamExt;
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

use crate::access_binding::IcebergReadBinding;
use crate::commit::{
    FileSet, compute_live_snapshot_set, enumerate_files_for_snapshots,
    puffin_half_reference_protection,
};
use crate::fs_io;
use crate::iceberg::io::FileIO;
use crate::iceberg::spec::TableMetadata;
use crate::iceberg::table::Table;
use novarocks_fs::{FsLocation, FsScheme};

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
    binding: &IcebergReadBinding,
    max_candidates: usize,
) -> Result<Vec<ScannedFile>, ConnectorError> {
    super::cleanup_maintenance::iceberg_scan_gc_safety().require_scanned_object_gc()?;
    let metadata = table.metadata();
    let file_io = table.file_io();
    let location = metadata.location().trim_end_matches('/');
    // Only snapshots reachable from the current Catalog refs are live. A
    // retired NovaRocks staging ref must therefore be followed by a fresh
    // metadata reload before the next object pass can see its files as
    // candidates; retaining every historical metadata snapshot would pin them
    // forever and defeat owned-ref GC.
    let snapshot_ids = compute_live_snapshot_set(metadata);
    let mut live: FileSet = enumerate_files_for_snapshots(file_io, metadata, &snapshot_ids)
        .await
        .map_err(|error| unavailable(error.to_string()))?;
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
            return Err(unavailable(format!(
                "orphan cleanup scan path `{child}` escapes table location `{location}`"
            )));
        }
    }
    let scanned = list_files(location, binding, &live, older_than_ms, max_candidates).await?;
    let mut candidate_paths = scanned
        .iter()
        .map(|file| file.path.clone())
        .collect::<FileSet>();
    let dv_index = build_dv_index(metadata, file_io, &snapshot_ids)
        .await
        .map_err(unavailable)?;
    puffin_half_reference_protection(&mut candidate_paths, &dv_index, &live);
    let mut selected = scanned
        .into_iter()
        .filter(|file| candidate_paths.contains(&file.path))
        .collect::<Vec<_>>();
    selected.sort_by(|left, right| left.path.cmp(&right.path));
    if selected.windows(2).any(|pair| pair[0].path == pair[1].path) {
        return Err(unavailable(
            "orphan cleanup scan produced duplicate locations",
        ));
    }
    Ok(selected)
}

async fn list_files(
    location: &str,
    binding: &IcebergReadBinding,
    live: &FileSet,
    older_than_ms: i64,
    max_candidates: usize,
) -> Result<Vec<ScannedFile>, ConnectorError> {
    super::cleanup_maintenance::iceberg_scan_gc_safety().require_scanned_object_gc()?;
    let parsed = FsLocation::parse(location).map_err(|error| {
        unavailable(format!(
            "parse orphan cleanup location `{location}`: {error}"
        ))
    })?;
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
                    walk_local(
                        &child,
                        prefix,
                        live,
                        older_than_ms,
                        max_candidates,
                        &mut files,
                    )?;
                }
            }
            Ok(files)
        }
        FsScheme::ObjectStore | FsScheme::Hdfs => {
            list_opendal(
                parsed.original(),
                binding,
                live,
                older_than_ms,
                max_candidates,
            )
            .await
        }
    }
}

fn walk_local(
    directory: &std::path::Path,
    prefix: &str,
    live: &FileSet,
    older_than_ms: i64,
    max_candidates: usize,
    files: &mut Vec<ScannedFile>,
) -> Result<(), ConnectorError> {
    for entry in std::fs::read_dir(directory)
        .map_err(|error| unavailable(format!("read orphan cleanup directory: {error}")))?
    {
        let entry =
            entry.map_err(|error| unavailable(format!("read orphan cleanup entry: {error}")))?;
        let file_type = entry
            .file_type()
            .map_err(|error| unavailable(format!("read orphan cleanup file type: {error}")))?;
        if file_type.is_dir() {
            walk_local(
                &entry.path(),
                prefix,
                live,
                older_than_ms,
                max_candidates,
                files,
            )?;
        } else if file_type.is_file() {
            let metadata = entry.metadata().ok();
            let mtime = metadata
                .as_ref()
                .and_then(|metadata| metadata.modified().ok())
                .and_then(|value| value.duration_since(std::time::UNIX_EPOCH).ok())
                .and_then(|value| i64::try_from(value.as_millis()).ok())
                .unwrap_or(i64::MAX);
            collect_candidate(
                ScannedFile {
                    path: format!("{prefix}{}", entry.path().to_string_lossy()),
                    last_modified_ms: mtime,
                    size: metadata.map(|metadata| metadata.len()),
                    etag: None,
                    version: None,
                },
                live,
                older_than_ms,
                max_candidates,
                files,
            )?;
        }
    }
    Ok(())
}

async fn list_opendal(
    location: &str,
    binding: &IcebergReadBinding,
    live: &FileSet,
    older_than_ms: i64,
    max_candidates: usize,
) -> Result<Vec<ScannedFile>, ConnectorError> {
    let access = fs_io::resolve_access_for_location(location, binding)
        .map_err(|error| unavailable(format!("resolve orphan cleanup location: {error}")))?;
    let operator = access.operator();
    let root = access
        .single_relative_path()
        .map_err(|error| unavailable(format!("resolve orphan cleanup key: {error}")))?
        .trim_matches('/')
        .to_string();
    let mut files = Vec::new();
    for child in ["data", "metadata"] {
        let prefix = if root.is_empty() {
            format!("{child}/")
        } else {
            format!("{root}/{child}/")
        };
        let mut entries = match list_candidate_entries(&operator, &prefix).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == crate::opendal::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(unavailable(format!(
                    "list orphan cleanup prefix `{prefix}`: {error}"
                )));
            }
        };
        while let Some(entry) = entries.try_next().await.map_err(|error| {
            unavailable(format!("list orphan cleanup prefix `{prefix}`: {error}"))
        })? {
            if entry.metadata().is_dir() {
                continue;
            }
            collect_candidate(
                ScannedFile {
                    path: fs_io::format_resolved_location(access.handle(), entry.path()).map_err(
                        |error| unavailable(format!("format orphan cleanup path: {error}")),
                    )?,
                    last_modified_ms: entry
                        .metadata()
                        .last_modified()
                        .map(|value| canonical_object_mtime_ms(value.into_inner().as_millisecond()))
                        .unwrap_or(i64::MAX),
                    size: Some(entry.metadata().content_length()),
                    etag: entry.metadata().etag().map(ToOwned::to_owned),
                    version: entry.metadata().version().map(ToOwned::to_owned),
                },
                live,
                older_than_ms,
                max_candidates,
                &mut files,
            )?;
        }
    }
    Ok(files)
}

async fn list_candidate_entries(
    operator: &crate::opendal::Operator,
    prefix: &str,
) -> Result<crate::opendal::Lister, crate::opendal::Error> {
    super::cleanup_maintenance::iceberg_scan_gc_safety()
        .require_scanned_object_gc()
        .map_err(|error| {
            crate::opendal::Error::new(crate::opendal::ErrorKind::Unsupported, error.message())
        })?;
    // Iceberg data, manifests, and document carriers live below nested paths.
    // A one-level listing sees only directory entries and would report an
    // empty orphan manifest for an object store table.
    operator.lister_with(prefix).recursive(true).await
}

fn collect_candidate(
    file: ScannedFile,
    live: &FileSet,
    older_than_ms: i64,
    max_candidates: usize,
    selected: &mut Vec<ScannedFile>,
) -> Result<(), ConnectorError> {
    let normalized = normalize_scanned_path(&file.path);
    if crate::catalog_metadata_root::is_catalog_metadata_root(&file.path)
        || file.last_modified_ms >= older_than_ms
        || live.contains(&file.path)
        || live.contains(&normalized)
    {
        return Ok(());
    }
    if selected.len() >= max_candidates {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "Iceberg orphan cleanup candidate scan exceeds its manifest record limit",
        ));
    }
    selected.push(file);
    Ok(())
}

fn unavailable(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message)
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
        let manifest_list = crate::manifest::load_manifest_list(snapshot, file_io, metadata)
            .await
            .map_err(|error| format!("load orphan cleanup manifest list: {error}"))?;
        for manifest_file in manifest_list.entries() {
            let manifest = crate::manifest::load_manifest(manifest_file, file_io)
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

#[cfg(test)]
mod tests {
    use crate::opendal::{Operator, services::Memory};

    use super::{ScannedFile, collect_candidate, list_candidate_entries};
    use crate::commit::FileSet;

    #[tokio::test]
    async fn unproven_listing_rejects_before_actual_list() {
        let base = Operator::new(Memory::default()).unwrap();
        base.write("warehouse/table/data/partition=1/file.parquet", vec![1])
            .await
            .unwrap();
        base.write("warehouse/table/metadata/scratch/object.bin", vec![2])
            .await
            .unwrap();
        let calls = super::gc_io_test::GcIoCalls::default();
        let operator = base.clone().layer(calls.clone());
        for prefix in ["warehouse/table/data/", "warehouse/table/metadata/"] {
            let error = list_candidate_entries(&operator, prefix)
                .await
                .err()
                .expect("unproven scan must be refused");
            assert_eq!(error.kind(), crate::opendal::ErrorKind::Unsupported);
        }
        assert_eq!(calls.counts(), (0, 0));
        assert_eq!(
            base.read("warehouse/table/data/partition=1/file.parquet")
                .await
                .unwrap()
                .to_bytes(),
            bytes::Bytes::from_static(&[1])
        );
    }

    #[test]
    fn catalog_version_roots_outside_metadata_log_are_not_orphan_candidates() {
        let mut selected = Vec::new();
        for name in [
            "v1.metadata.json",
            "v22.gz.metadata.json",
            "v9.metadata.json.gz",
            "version-hint.text",
        ] {
            collect_candidate(
                ScannedFile {
                    path: format!("s3://bucket/other/metadata/{name}"),
                    last_modified_ms: 0,
                    size: Some(1),
                    etag: None,
                    version: None,
                },
                &FileSet::new(),
                10,
                1,
                &mut selected,
            )
            .unwrap();
        }
        assert!(
            selected.is_empty(),
            "catalog roots need no metadata-log entry"
        );
        for (path, mtime) in [
            ("s3://bucket/t/metadata/install-temp.tmp", 10),
            ("s3://bucket/t/data/v1.metadata.json", 0),
        ] {
            collect_candidate(
                ScannedFile {
                    path: path.into(),
                    last_modified_ms: mtime,
                    size: Some(1),
                    etag: None,
                    version: None,
                },
                &FileSet::new(),
                10,
                1,
                &mut selected,
            )
            .unwrap();
        }
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].path, "s3://bucket/t/data/v1.metadata.json");
    }

    #[test]
    fn candidate_collection_filters_before_the_manifest_limit() {
        let live = FileSet::from(["s3://warehouse/table/data/live.parquet".to_string()]);
        let mut selected = Vec::new();
        let file = |path: &str, mtime| ScannedFile {
            path: path.to_string(),
            last_modified_ms: mtime,
            size: Some(1),
            etag: None,
            version: None,
        };
        collect_candidate(
            file("s3://warehouse/table/data/live.parquet", 1),
            &live,
            10,
            1,
            &mut selected,
        )
        .expect("live file is ignored");
        collect_candidate(
            file("s3://warehouse/table/data/new.parquet", 10),
            &live,
            10,
            1,
            &mut selected,
        )
        .expect("young file is ignored");
        collect_candidate(
            file("s3://warehouse/table/data/old.parquet", 1),
            &live,
            10,
            1,
            &mut selected,
        )
        .expect("old orphan fits");
        let error = collect_candidate(
            file("s3://warehouse/table/data/another.parquet", 1),
            &live,
            10,
            1,
            &mut selected,
        )
        .expect_err("second orphan exceeds the manifest bound");
        assert_eq!(
            error.kind(),
            novarocks_spi::connector::ConnectorErrorKind::ResourceExhausted
        );
        assert_eq!(selected.len(), 1);
    }
}

#[cfg(test)]
pub(super) mod gc_io_test {
    use crate::opendal::{self, OperationContext, raw::*};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    #[derive(Clone, Debug, Default)]
    pub(in crate::catalog_control) struct GcIoCalls {
        lists: Arc<AtomicUsize>,
        deletes: Arc<AtomicUsize>,
    }
    impl GcIoCalls {
        pub(in crate::catalog_control) fn counts(&self) -> (usize, usize) {
            (
                self.lists.load(Ordering::SeqCst),
                self.deletes.load(Ordering::SeqCst),
            )
        }
    }
    impl Layer for GcIoCalls {
        fn apply_service(&self, inner: Servicer) -> Servicer {
            Arc::new(RecordedGcAccess {
                inner,
                calls: self.clone(),
            })
        }
    }
    #[derive(Debug)]
    struct RecordedGcAccess {
        inner: Servicer,
        calls: GcIoCalls,
    }
    impl Service for RecordedGcAccess {
        type Reader = oio::Reader;
        type Writer = oio::Writer;
        type Lister = oio::Lister;
        type Deleter = oio::Deleter;
        type Copier = oio::Copier;
        fn info(&self) -> ServiceInfo {
            self.inner.info()
        }
        fn capability(&self) -> opendal::Capability {
            self.inner.capability()
        }
        async fn create_dir(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpCreateDir,
        ) -> opendal::Result<RpCreateDir> {
            self.inner.create_dir(ctx, path, args).await
        }
        async fn stat(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpStat,
        ) -> opendal::Result<RpStat> {
            self.inner.stat(ctx, path, args).await
        }
        fn read(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpRead,
        ) -> opendal::Result<Self::Reader> {
            self.inner.read(ctx, path, args)
        }
        fn write(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpWrite,
        ) -> opendal::Result<Self::Writer> {
            self.inner.write(ctx, path, args)
        }
        fn delete(&self, ctx: &OperationContext) -> opendal::Result<Self::Deleter> {
            self.calls.deletes.fetch_add(1, Ordering::SeqCst);
            self.inner.delete(ctx)
        }
        fn list(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpList,
        ) -> opendal::Result<Self::Lister> {
            self.calls.lists.fetch_add(1, Ordering::SeqCst);
            self.inner.list(ctx, path, args)
        }
        fn copy(
            &self,
            ctx: &OperationContext,
            from: &str,
            to: &str,
            args: OpCopy,
            opts: OpCopier,
        ) -> opendal::Result<Self::Copier> {
            self.inner.copy(ctx, from, to, args, opts)
        }
        async fn rename(
            &self,
            ctx: &OperationContext,
            from: &str,
            to: &str,
            args: OpRename,
        ) -> opendal::Result<RpRename> {
            self.inner.rename(ctx, from, to, args).await
        }
        async fn presign(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpPresign,
        ) -> opendal::Result<RpPresign> {
            self.inner.presign(ctx, path, args).await
        }
    }
}
