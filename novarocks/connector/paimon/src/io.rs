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
use std::ops::Range;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::{Stream, StreamExt};
use novarocks_fs::{
    FileCancellation, FileError, FileErrorKind, FileIdentity, FileRangeBinding, FileReadRange,
    FileResult, FsAccessHandle, FsLocation,
};
use novarocks_spi::connector::ConnectorError;
use paimon::io::{
    FileStatus, FileStatusStream, ReadExecutionResources, ReadOnlyFileIO, retain_bytes,
};

use crate::resources::PaimonExecutionResources;
use crate::sdk_control::PaimonSdkExecutionResources;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaimonListedEntry {
    pub path: String,
    pub size: u64,
    pub is_dir: bool,
}

pub type PaimonListingStream =
    Pin<Box<dyn Stream<Item = FileResult<PaimonListedEntry>> + Send + 'static>>;

/// Minimal authorized listing capability required from novarocks-fs.
///
/// The main integration implements this over one already-resolved access
/// handle. It must paginate and must never manufacture credentials.
#[async_trait::async_trait]
pub trait PaimonAuthorizedListing: std::fmt::Debug + Send + Sync {
    async fn list(
        &self,
        access: &FsAccessHandle,
        prefix: &str,
        recursive: bool,
        cancellation: &FileCancellation,
    ) -> FileResult<PaimonListingStream>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PaimonFsAuthorizedListing;

#[async_trait::async_trait]
impl PaimonAuthorizedListing for PaimonFsAuthorizedListing {
    async fn list(
        &self,
        access: &FsAccessHandle,
        prefix: &str,
        recursive: bool,
        cancellation: &FileCancellation,
    ) -> FileResult<PaimonListingStream> {
        let stream = access
            .list_location(prefix, recursive, cancellation)
            .await?;
        Ok(Box::pin(stream.map(|entry| {
            entry.map(|entry| PaimonListedEntry {
                path: entry.location().to_string(),
                size: entry.size(),
                is_dir: entry.is_dir(),
            })
        })))
    }
}

#[derive(Clone)]
pub struct PaimonHostFileIo {
    access: FsAccessHandle,
    warehouse: FsLocation,
    cancellation: FileCancellation,
    listing: Arc<dyn PaimonAuthorizedListing>,
    /// On an execution attempt, the shared scan I/O every HEAD and GET goes
    /// through, bound to the source the attempt reads for.
    range: Option<FileRangeBinding>,
    /// Sizes of the immutable objects this session reads, shared by every
    /// clone: a split's frozen data files, and each object it had to probe.
    sizes: Arc<ObjectSizes>,
}

#[derive(Debug, Default)]
struct ObjectSizes {
    known: Mutex<HashMap<String, u64>>,
    probes: AtomicU64,
}

impl PaimonHostFileIo {
    pub fn try_new(
        access: FsAccessHandle,
        warehouse: impl AsRef<str>,
        cancellation: FileCancellation,
        listing: Arc<dyn PaimonAuthorizedListing>,
    ) -> FileResult<Self> {
        let warehouse = FsLocation::parse(warehouse)?;
        if warehouse.scheme() != access.scheme() || warehouse.authority() != access.authority() {
            return Err(FileError::new(
                FileErrorKind::Permission,
                "Paimon warehouse is outside the authorized filesystem domain",
            ));
        }
        Ok(Self {
            access,
            warehouse,
            cancellation,
            listing,
            range: None,
            sizes: Arc::default(),
        })
    }

    /// Records the frozen size of an object this session reads, so no read
    /// of it probes the size. An object named with two different sizes is
    /// refused rather than read as either.
    pub fn know_object_size(&self, path: &str, size: u64) -> FileResult<()> {
        self.validate_location(path)?;
        self.remember_size(path, size)
    }

    /// How many objects this session had to probe for their size.
    pub fn size_probes(&self) -> u64 {
        self.sizes.probes.load(Ordering::Acquire)
    }

    fn remember_size(&self, path: &str, size: u64) -> FileResult<()> {
        let mut known = self.sizes.known.lock().map_err(|_| registry_poisoned())?;
        match known.get(path) {
            Some(existing) if *existing != size => Err(size_conflict(path, *existing, size)),
            Some(_) => Ok(()),
            None => {
                known.insert(path.to_string(), size);
                Ok(())
            }
        }
    }

    fn remembered_size(&self, path: &str) -> FileResult<Option<u64>> {
        self.sizes
            .known
            .lock()
            .map(|known| known.get(path).copied())
            .map_err(|_| registry_poisoned())
    }

    /// The object's size: the registered one or the caller's, which must
    /// agree, and a managed HEAD only when neither is known.
    async fn object_size(&self, path: &str, known_size: Option<u64>) -> FileResult<u64> {
        self.validate_location(path)?;
        let size = match (self.remembered_size(path)?, known_size) {
            (Some(registered), Some(known)) if registered != known => {
                return Err(size_conflict(path, registered, known));
            }
            (Some(size), _) => return Ok(size),
            (None, Some(size)) => size,
            (None, None) => {
                self.sizes.probes.fetch_add(1, Ordering::AcqRel);
                self.stat_size(path).await?
            }
        };
        self.remember_size(path, size)?;
        Ok(size)
    }

    /// Sends every HEAD and GET through the shared scan I/O of the one source
    /// this attempt reads for, under its windows, fairness and exit accounting.
    pub fn with_range_binding(mut self, range: FileRangeBinding) -> Self {
        self.range = Some(range);
        self
    }

    fn validate_location(&self, path: &str) -> FileResult<FsLocation> {
        if path.as_bytes().contains(&b'\\') {
            return Err(FileError::invalid(
                "Paimon object path contains a backslash alias",
            ));
        }
        let location = FsLocation::parse(path)?;
        if location.scheme() != self.warehouse.scheme()
            || location.authority() != self.warehouse.authority()
        {
            return Err(FileError::new(
                FileErrorKind::Permission,
                "Paimon object is outside the authorized filesystem domain",
            ));
        }
        let base = self.warehouse.path().trim_matches('/');
        let candidate = location.path().trim_matches('/');
        let inside_warehouse = base.is_empty()
            || candidate == base
            || candidate
                .strip_prefix(base)
                .is_some_and(|suffix| suffix.starts_with('/'));
        if candidate.split('/').any(|part| part == "." || part == "..") || !inside_warehouse {
            return Err(FileError::new(
                FileErrorKind::Permission,
                "Paimon object escapes the authorized warehouse",
            ));
        }
        Ok(location)
    }

    async fn stat_size(&self, path: &str) -> FileResult<u64> {
        self.validate_location(path)?;
        let probe = self
            .access
            .bind_location(path, FileIdentity::new(path, 0, None))?;
        let Some(range) = &self.range else {
            return probe.stat(&self.cancellation).await;
        };
        let mut request = range.stat_wait(probe, self.cancellation.clone()).await?;
        let size = request.size_ready().await;
        let exit = request.drained().await;
        let size = size?;
        exit?;
        Ok(size)
    }

    async fn read_range(&self, path: &str, size: u64, range: Range<u64>) -> FileResult<Bytes> {
        let bytes = self.read_range_unchecked(path, size, range.clone()).await?;
        let requested = range.end - range.start;
        if bytes.len() as u64 != requested {
            return Err(FileError::new(
                FileErrorKind::Corrupt,
                format!(
                    "Paimon object {path} returned {} bytes for a {requested}-byte range",
                    bytes.len()
                ),
            ));
        }
        Ok(bytes)
    }

    async fn read_range_unchecked(
        &self,
        path: &str,
        size: u64,
        range: Range<u64>,
    ) -> FileResult<Bytes> {
        if range.is_empty() {
            return Ok(Bytes::new());
        }
        let file = self
            .access
            .bind_location(path, FileIdentity::new(path, size, None))?;
        let read = FileReadRange::Bounded {
            offset: range.start,
            length: range.end - range.start,
        };
        let Some(range) = &self.range else {
            return file.read(read, &self.cancellation).await;
        };
        let mut request = range
            .start_wait(file, read, self.cancellation.clone())
            .await?;
        let bytes = request.result_ready().await;
        let exit = request.drained().await;
        let bytes = bytes?;
        exit?;
        Ok(bytes)
    }
}

impl std::fmt::Debug for PaimonHostFileIo {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PaimonHostFileIo")
            .field("access", &self.access)
            .field("warehouse", &self.warehouse.original())
            .field("cancellation", &self.cancellation)
            .field("range", &self.range)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl ReadOnlyFileIO for PaimonHostFileIo {
    async fn stat(&self, path: &str) -> paimon::Result<FileStatus> {
        let size = self.object_size(path, None).await.map_err(map_file_error)?;
        Ok(FileStatus {
            size,
            is_dir: false,
            path: path.to_string(),
            last_modified: None,
        })
    }

    async fn exists(&self, path: &str) -> paimon::Result<bool> {
        match self.object_size(path, None).await {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == FileErrorKind::NotFound => Ok(false),
            Err(error) => Err(map_file_error(error)),
        }
    }

    async fn read(
        &self,
        path: &str,
        range: Range<u64>,
        known_size: Option<u64>,
    ) -> paimon::Result<Bytes> {
        let size = self
            .object_size(path, known_size)
            .await
            .map_err(map_file_error)?;
        if range.start > range.end || range.end > size {
            return Err(paimon::Error::ConfigInvalid {
                message: "Paimon host read range is outside the frozen object".to_string(),
            });
        }
        self.read_range(path, size, range)
            .await
            .map_err(map_file_error)
    }

    async fn list(&self, path: &str, recursive: bool) -> paimon::Result<FileStatusStream> {
        self.validate_location(path).map_err(map_file_error)?;
        self.cancellation.check().map_err(map_file_error)?;
        let stream = self
            .listing
            .list(&self.access, path, recursive, &self.cancellation)
            .await
            .map_err(map_file_error)?;
        Ok(Box::pin(stream.map(|entry| {
            entry
                .map(|entry| FileStatus {
                    size: entry.size,
                    is_dir: entry.is_dir,
                    path: entry.path,
                    last_modified: None,
                })
                .map_err(map_file_error)
        })))
    }
}

/// BE-only file adapter. Each returned byte buffer owns its admitted read
/// reservation until the last `Bytes` clone is dropped.
#[derive(Clone, Debug)]
pub struct PaimonChargedHostFileIo {
    inner: PaimonHostFileIo,
    resources: PaimonExecutionResources,
    sdk_resources: Arc<PaimonSdkExecutionResources>,
}

impl PaimonChargedHostFileIo {
    pub fn new(
        inner: PaimonHostFileIo,
        resources: PaimonExecutionResources,
        sdk_resources: Arc<PaimonSdkExecutionResources>,
    ) -> Self {
        Self {
            inner,
            resources,
            sdk_resources,
        }
    }
}

#[async_trait::async_trait]
impl ReadOnlyFileIO for PaimonChargedHostFileIo {
    async fn stat(&self, path: &str) -> paimon::Result<FileStatus> {
        self.resources.checkpoint().map_err(map_execution_error)?;
        let result = self.inner.stat(path).await?;
        self.resources.checkpoint().map_err(map_execution_error)?;
        Ok(result)
    }

    async fn exists(&self, path: &str) -> paimon::Result<bool> {
        self.resources.checkpoint().map_err(map_execution_error)?;
        let result = self.inner.exists(path).await?;
        self.resources.checkpoint().map_err(map_execution_error)?;
        Ok(result)
    }

    async fn read(
        &self,
        path: &str,
        range: Range<u64>,
        known_size: Option<u64>,
    ) -> paimon::Result<Bytes> {
        self.resources.checkpoint().map_err(map_execution_error)?;
        let requested =
            range
                .end
                .checked_sub(range.start)
                .ok_or_else(|| paimon::Error::ConfigInvalid {
                    message: "Paimon read range end precedes start".to_string(),
                })?;
        let reservation = self.sdk_resources.try_reserve(requested.max(1))?;
        let bytes = self.inner.read(path, range, known_size).await?;
        self.resources.checkpoint().map_err(map_execution_error)?;
        Ok(retain_bytes(bytes, reservation))
    }

    async fn list(&self, _path: &str, _recursive: bool) -> paimon::Result<FileStatusStream> {
        Err(paimon::Error::IoUnsupported {
            message: "Paimon execution reader cannot list metadata".to_string(),
        })
    }
}

fn registry_poisoned() -> FileError {
    FileError::new(
        FileErrorKind::Internal,
        "Paimon object size registry lock was poisoned",
    )
}

fn size_conflict(path: &str, registered: u64, declared: u64) -> FileError {
    FileError::new(
        FileErrorKind::Corrupt,
        format!(
            "Paimon object {path} is declared as {declared} bytes but is known as {registered} bytes"
        ),
    )
}

fn map_execution_error(error: ConnectorError) -> paimon::Error {
    paimon::Error::UnexpectedError {
        message: "admitted Paimon execution resource rejected file operation".to_string(),
        source: Some(Box::new(error)),
    }
}

fn map_file_error(error: FileError) -> paimon::Error {
    match error.kind() {
        FileErrorKind::Unsupported => paimon::Error::IoUnsupported {
            message: error.to_string(),
        },
        FileErrorKind::Invalid => paimon::Error::ConfigInvalid {
            message: error.to_string(),
        },
        _ => paimon::Error::UnexpectedError {
            message: "host-authorized Paimon file operation failed".to_string(),
            source: Some(Box::new(error)),
        },
    }
}

pub(crate) fn connector_error_from_file_error(error: &FileError) -> ConnectorError {
    ConnectorError::from(error)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use novarocks_fs::{
        FileCancellation, FileError, FileErrorKind, FileRangeScope, FileRangeService,
        FsAccessResolver, TokioFileTaskSpawner,
    };
    use novarocks_spi::connector::read_stack::ConnectorSourceOperations;
    use novarocks_spi::connector::{ConnectorErrorKind, StorageAccessDomainId};
    use paimon::io::ReadOnlyFileIO;

    use super::{
        PaimonFsAuthorizedListing, PaimonHostFileIo, connector_error_from_file_error,
        map_file_error,
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_attempt_heads_and_reads_through_its_source_scan_io() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let warehouse = directory.path().join("warehouse");
        std::fs::create_dir_all(warehouse.join("bucket-0")).expect("warehouse");
        let data = warehouse.join("bucket-0").join("data-0.parquet");
        std::fs::write(&data, b"paimon-bytes").expect("data file");
        let warehouse = warehouse.to_string_lossy().to_string();
        let data = data.to_string_lossy().to_string();
        let access = FsAccessResolver::new()
            .resolve_location(StorageAccessDomainId::from_bytes([3; 32]), &data, None)
            .expect("local access");
        let handle = tokio::runtime::Handle::current();
        let service = FileRangeService::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(4).unwrap(),
            Arc::new(TokioFileTaskSpawner::new(handle.clone())),
            handle,
        );
        let operations = ConnectorSourceOperations::new();
        let file_io = PaimonHostFileIo::try_new(
            access,
            &warehouse,
            FileCancellation::new(),
            Arc::new(PaimonFsAuthorizedListing),
        )
        .expect("host file io")
        .with_range_binding(service.bind(
            FileRangeScope::try_new(1, 0, 1, 2, 0, 3).expect("scope"),
            operations.clone(),
        ));

        assert_eq!(file_io.stat(&data).await.expect("HEAD").size, 12);
        assert_eq!(
            &file_io.read(&data, 7..12, None).await.expect("GET")[..],
            b"bytes"
        );
        assert!(
            file_io
                .read(&data, 3..3, None)
                .await
                .expect("empty")
                .is_empty()
        );
        assert_eq!(file_io.size_probes(), 1, "the reads reuse the HEAD's size");
        assert_eq!(
            operations.live_operations(),
            0,
            "every HEAD and GET exited before it returned"
        );

        operations.seal();
        let error = file_io
            .read(&data, 0..6, None)
            .await
            .expect_err("a closed source admits no GET");
        let detail = format!("{error:?}");
        assert!(detail.contains("source is closed"), "{detail}");
        service.drain().await.expect("scan I/O drained");
    }

    #[test]
    fn preserves_host_cancellation_and_deadline_classification() {
        assert_eq!(
            connector_error_from_file_error(&FileError::cancelled("cancelled")).kind(),
            ConnectorErrorKind::Cancelled
        );
        assert_eq!(
            connector_error_from_file_error(&FileError::deadline("deadline")).kind(),
            ConnectorErrorKind::DeadlineExceeded
        );
    }

    #[test]
    fn preserves_host_permission_through_sdk_error_mapping() {
        let metadata = crate::catalog::map_sdk_error(map_file_error(FileError::new(
            novarocks_fs::FileErrorKind::Permission,
            "credential scope rejected",
        )));
        let reader = crate::reader::map_paimon_error(map_file_error(FileError::new(
            novarocks_fs::FileErrorKind::Permission,
            "credential scope rejected",
        )));

        assert_eq!(metadata.kind(), ConnectorErrorKind::PermissionDenied);
        assert_eq!(reader.kind(), ConnectorErrorKind::PermissionDenied);
    }

    /// A warehouse on local disk, read through a range service as the source
    /// of one attempt.
    struct LocalWarehouse {
        _directory: tempfile::TempDir,
        root: String,
        service: Arc<FileRangeService>,
        operations: ConnectorSourceOperations,
        file_io: PaimonHostFileIo,
    }

    impl LocalWarehouse {
        /// `files` are paths relative to the warehouse, with their bytes.
        fn with_files(files: &[(&str, &[u8])]) -> Self {
            let directory = tempfile::tempdir().expect("temporary directory");
            let root = directory.path().join("warehouse");
            for (path, bytes) in files {
                let path = root.join(path);
                std::fs::create_dir_all(path.parent().expect("parent")).expect("directory");
                std::fs::write(&path, bytes).expect("file");
            }
            let root = root.to_string_lossy().to_string();
            let access = FsAccessResolver::new()
                .resolve_location(StorageAccessDomainId::from_bytes([3; 32]), &root, None)
                .expect("local access");
            let handle = tokio::runtime::Handle::current();
            let service = FileRangeService::new(
                NonZeroUsize::new(2).unwrap(),
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(4).unwrap(),
                Arc::new(TokioFileTaskSpawner::new(handle.clone())),
                handle,
            );
            let operations = ConnectorSourceOperations::new();
            let file_io = PaimonHostFileIo::try_new(
                access,
                &root,
                FileCancellation::new(),
                Arc::new(PaimonFsAuthorizedListing),
            )
            .expect("host file io")
            .with_range_binding(service.bind(
                FileRangeScope::try_new(1, 0, 1, 2, 0, 3).expect("scope"),
                operations.clone(),
            ));
            Self {
                _directory: directory,
                root,
                service,
                operations,
                file_io,
            }
        }

        fn path(&self, relative: &str) -> String {
            format!("{}/{relative}", self.root)
        }

        async fn finish(self) {
            assert_eq!(self.operations.live_operations(), 0);
            self.service.drain().await.expect("scan I/O drained");
        }
    }

    fn detail(error: paimon::Error) -> String {
        format!("{error:?}")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_frozen_object_is_read_in_many_ranges_without_a_size_probe() {
        let warehouse = LocalWarehouse::with_files(&[("bucket-0/data-0.parquet", b"paimon-bytes")]);
        let data = warehouse.path("bucket-0/data-0.parquet");
        warehouse
            .file_io
            .know_object_size(&data, 12)
            .expect("frozen size");
        for (range, expected) in [(0..6, &b"paimon"[..]), (6..7, b"-"), (7..12, b"bytes")] {
            assert_eq!(
                &warehouse
                    .file_io
                    .read(&data, range, None)
                    .await
                    .expect("GET")[..],
                expected
            );
        }
        // The SDK's own, agreeing knowledge of the size probes nothing either.
        assert_eq!(
            &warehouse
                .file_io
                .read(&data, 0..6, Some(12))
                .await
                .expect("GET")[..],
            b"paimon"
        );
        assert_eq!(warehouse.file_io.stat(&data).await.expect("stat").size, 12);
        assert_eq!(warehouse.file_io.size_probes(), 0);
        warehouse.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unknown_object_is_probed_once_for_the_whole_session() {
        let warehouse = LocalWarehouse::with_files(&[("schema/schema-0", b"{\"id\":0}")]);
        let schema = warehouse.path("schema/schema-0");
        assert_eq!(warehouse.file_io.stat(&schema).await.expect("HEAD").size, 8);
        // Another clone of the session's host I/O: it knows the size too.
        let clone = warehouse.file_io.clone();
        assert_eq!(
            &clone.read(&schema, 0..8, None).await.expect("GET")[..],
            b"{\"id\":0}"
        );
        assert!(warehouse.file_io.exists(&schema).await.expect("exists"));
        assert_eq!(
            warehouse.file_io.size_probes(),
            1,
            "one HEAD, after which the session knows the size"
        );
        warehouse.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn objects_sharing_a_basename_in_two_places_keep_their_own_sizes() {
        let warehouse = LocalWarehouse::with_files(&[
            ("bucket-0/data-0.parquet", b"in-bucket"),
            ("external/data-0.parquet", b"external-file"),
        ]);
        let in_bucket = warehouse.path("bucket-0/data-0.parquet");
        let external = warehouse.path("external/data-0.parquet");
        warehouse
            .file_io
            .know_object_size(&in_bucket, 9)
            .expect("bucket file size");
        warehouse
            .file_io
            .know_object_size(&external, 13)
            .expect("external file size");
        assert_eq!(
            &warehouse
                .file_io
                .read(&in_bucket, 0..9, Some(9))
                .await
                .expect("GET")[..],
            b"in-bucket"
        );
        assert_eq!(
            &warehouse
                .file_io
                .read(&external, 0..13, Some(13))
                .await
                .expect("GET")[..],
            b"external-file"
        );
        assert_eq!(warehouse.file_io.size_probes(), 0);
        warehouse.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_size_conflict_a_range_past_the_object_and_a_short_read_are_refused() {
        let warehouse = LocalWarehouse::with_files(&[
            ("bucket-0/data-0.parquet", b"paimon-bytes"),
            ("bucket-0/data-1.parquet", b"paimon-bytes"),
        ]);
        let data = warehouse.path("bucket-0/data-0.parquet");
        warehouse
            .file_io
            .know_object_size(&data, 12)
            .expect("frozen size");
        let conflict = warehouse
            .file_io
            .know_object_size(&data, 13)
            .expect_err("a second size for one object");
        assert_eq!(conflict.kind(), FileErrorKind::Corrupt);
        let conflict = warehouse
            .file_io
            .read(&data, 0..6, Some(13))
            .await
            .expect_err("a read that disagrees about the size");
        assert!(
            detail(conflict).contains("is declared as 13 bytes but is known as 12 bytes"),
            "the conflict names both sizes"
        );
        let past = warehouse
            .file_io
            .read(&data, 8..16, None)
            .await
            .expect_err("a range past the object");
        assert!(detail(past).contains("outside the frozen object"));

        // A frozen size larger than the object: the read comes back short.
        let short = warehouse.path("bucket-0/data-1.parquet");
        warehouse
            .file_io
            .know_object_size(&short, 20)
            .expect("an overstated size");
        warehouse
            .file_io
            .read(&short, 0..20, None)
            .await
            .expect_err("a short read is not the requested bytes");
        assert_eq!(warehouse.file_io.size_probes(), 0);
        warehouse.finish().await;
    }
}
