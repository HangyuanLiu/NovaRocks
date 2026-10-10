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
    FileResult, FsAccessHandle, FsListingBound, FsLocation,
};
use novarocks_spi::connector::ConnectorError;
use paimon::io::{
    FileStatus, FileStatusStream, ReadExecutionResources, ReadOnlyFileIO, ReadReservation,
    retain_bytes,
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

    /// Explicit local-catalog source capability. Implementations must bound
    /// growth before forming owned paths; an unbounded implementation cannot
    /// serve this call through a fallback.
    async fn list_bounded(
        &self,
        _access: &FsAccessHandle,
        _prefix: &str,
        _recursive: bool,
        _cancellation: &FileCancellation,
        _bound: FsListingBound,
    ) -> FileResult<PaimonListingStream> {
        Err(FileError::unsupported(
            "Paimon listing backend has no bounded catalog listing capability",
        ))
    }
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
            entry.map(|entry| {
                let (path, size, is_dir) = entry.into_parts();
                PaimonListedEntry { path, size, is_dir }
            })
        })))
    }

    async fn list_bounded(
        &self,
        access: &FsAccessHandle,
        prefix: &str,
        recursive: bool,
        cancellation: &FileCancellation,
        bound: FsListingBound,
    ) -> FileResult<PaimonListingStream> {
        let stream = access
            .list_location_bounded(prefix, recursive, cancellation, bound)
            .await?;
        Ok(Box::pin(stream.map(|entry| {
            entry.map(|entry| {
                let (path, size, is_dir) = entry.into_parts();
                PaimonListedEntry { path, size, is_dir }
            })
        })))
    }
}

#[derive(Clone)]
pub struct PaimonHostFileIo {
    access: Arc<FsAccessHandle>,
    warehouse: Arc<FsLocation>,
    cancellation: FileCancellation,
    listing: Arc<dyn PaimonAuthorizedListing>,
    listing_bound: Option<FsListingBound>,
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
            access: Arc::new(access),
            warehouse: Arc::new(warehouse),
            cancellation,
            listing,
            listing_bound: None,
            range: None,
            sizes: Arc::default(),
        })
    }

    /// Opt this host clone into an explicitly admitted local catalog listing.
    /// Access and warehouse facts stay shared with the same role-local owner;
    /// this does not clone an inventory or change its credential/renewal path.
    pub fn with_listing_bound(mut self, bound: FsListingBound) -> Self {
        self.listing_bound = Some(bound);
        self
    }

    /// A clone whose reads are admitted to their own child of the attempt's
    /// source, so closing one split stops and observes exactly its reads;
    /// it shares this session's object sizes. `None` without a range service.
    pub fn bind_split_operations(
        &self,
    ) -> Result<
        (
            Self,
            Option<novarocks_spi::connector::read_stack::ConnectorSourceOperations>,
        ),
        ConnectorError,
    > {
        let mut split = self.clone();
        let Some(range) = &self.range else {
            return Ok((split, None));
        };
        let operations = range.operations().child()?;
        split.range = Some(range.with_operations(operations.clone()));
        Ok((split, Some(operations)))
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
        if let Some(bound) = self.bounded_catalog_path(path)? {
            if known_size.is_some() {
                return Err(FileError::invalid(
                    "bounded catalog metadata probe cannot accept a frozen read size",
                ));
            }
            self.validate_location(path)?;
            self.cancellation.check()?;
            self.sizes.probes.fetch_add(1, Ordering::AcqRel);
            // Metadata existence is fresh per call, not a frozen scan fact.
            // Neither lookup nor insertion touches the shared scan-size cache.
            return self
                .access
                .stat_location_bounded(path, &self.cancellation, bound)
                .await;
        }
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

    fn bounded_catalog_path(&self, path: &str) -> FileResult<Option<FsListingBound>> {
        if let Some(bound) = self.listing_bound {
            if self.range.is_some() {
                return Err(FileError::invalid(
                    "bounded catalog metadata cannot share a BE range-service binding",
                ));
            }
            // Admit before validate_location/identity/path or metadata copies.
            bound.check_path(path)?;
        }
        Ok(self.listing_bound)
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

    /// Reads `range` like [`ReadOnlyFileIO::read`]. A managed read keeps
    /// `hold` alive until its physical exit, even when this future is
    /// dropped first.
    pub(crate) async fn read_holding(
        &self,
        path: &str,
        range: Range<u64>,
        known_size: Option<u64>,
        hold: Option<ReadHold>,
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
        self.read_range(path, size, range, hold)
            .await
            .map_err(map_file_error)
    }

    async fn read_range(
        &self,
        path: &str,
        size: u64,
        range: Range<u64>,
        hold: Option<ReadHold>,
    ) -> FileResult<Bytes> {
        let bytes = self
            .read_range_unchecked(path, size, range.clone(), hold)
            .await?;
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
        hold: Option<ReadHold>,
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
        if let Some(hold) = hold {
            request.retain_until_exit(Box::new(hold));
        }
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
        self.bounded_catalog_path(path).map_err(map_file_error)?;
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
        self.read_holding(path, range, known_size, None).await
    }

    async fn list(&self, path: &str, recursive: bool) -> paimon::Result<FileStatusStream> {
        self.bounded_catalog_path(path).map_err(map_file_error)?;
        self.validate_location(path).map_err(map_file_error)?;
        self.cancellation.check().map_err(map_file_error)?;
        let stream = match self.listing_bound {
            Some(bound) => {
                self.listing
                    .list_bounded(&self.access, path, recursive, &self.cancellation, bound)
                    .await
            }
            None => {
                self.listing
                    .list(&self.access, path, recursive, &self.cancellation)
                    .await
            }
        }
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
        let reservation = SharedReservation::new(self.sdk_resources.try_reserve(requested.max(1))?);
        // The physical read keeps the charge until it exits and the bytes
        // keep it until their last clone is dropped, whichever is later, so
        // a read dropped mid-flight still pays for what its task holds.
        let bytes = self
            .inner
            .read_holding(path, range, known_size, Some(reservation.hold()))
            .await?;
        self.resources.checkpoint().map_err(map_execution_error)?;
        Ok(retain_bytes(bytes, Box::new(reservation)))
    }

    async fn list(&self, _path: &str, _recursive: bool) -> paimon::Result<FileStatusStream> {
        Err(paimon::Error::IoUnsupported {
            message: "Paimon execution reader cannot list metadata".to_string(),
        })
    }
}

/// A charge that must stay alive while a physical read can still hold the
/// bytes it pays for.
pub(crate) type ReadHold = Arc<dyn std::any::Any + Send + Sync>;

/// One read reservation shared by the physical read and the bytes it
/// returns; it is released when both are gone.
#[derive(Clone)]
struct SharedReservation(Arc<Mutex<Box<dyn ReadReservation>>>);

impl SharedReservation {
    fn new(reservation: Box<dyn ReadReservation>) -> Self {
        Self(Arc::new(Mutex::new(reservation)))
    }

    fn hold(&self) -> ReadHold {
        Arc::new(self.clone())
    }
}

impl std::fmt::Debug for SharedReservation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("SharedReservation")
            .field(&ReadReservation::bytes(self))
            .finish()
    }
}

impl ReadReservation for SharedReservation {
    fn bytes(&self) -> u64 {
        self.0.lock().map_or(0, |reservation| reservation.bytes())
    }

    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
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
        FileCancellation, FileError, FileErrorKind, FileRangeScope, FileRangeService, FileResult,
        FileTask, FileTaskFuture, FileTaskSpawner, FsAccessResolver, TokioFileTaskSpawner,
    };
    use novarocks_spi::connector::read_stack::ConnectorSourceOperations;
    use novarocks_spi::connector::{ConnectorErrorKind, StorageAccessDomainId};
    use paimon::io::ReadOnlyFileIO;

    use super::{
        PaimonChargedHostFileIo, PaimonFsAuthorizedListing, PaimonHostFileIo,
        connector_error_from_file_error, map_file_error,
    };

    #[derive(Debug, Default)]
    struct GenericOnlyListing {
        calls: std::sync::atomic::AtomicUsize,
    }
    #[async_trait::async_trait]
    impl super::PaimonAuthorizedListing for GenericOnlyListing {
        async fn list(
            &self,
            _access: &novarocks_fs::FsAccessHandle,
            _prefix: &str,
            _recursive: bool,
            _cancellation: &FileCancellation,
        ) -> FileResult<super::PaimonListingStream> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(Box::pin(futures::stream::empty()))
        }
    }
    fn listing_host(
        listing: Arc<dyn super::PaimonAuthorizedListing>,
    ) -> (tempfile::TempDir, PaimonHostFileIo, String) {
        let directory = tempfile::tempdir().unwrap();
        let warehouse = directory.path().join("warehouse");
        std::fs::create_dir_all(&warehouse).unwrap();
        let warehouse = warehouse.to_string_lossy().into_owned();
        let access = FsAccessResolver::new()
            .resolve_location(
                StorageAccessDomainId::from_bytes([19; 32]),
                &warehouse,
                None,
            )
            .unwrap();
        let host = PaimonHostFileIo::try_new(access, &warehouse, FileCancellation::new(), listing)
            .unwrap();
        (directory, host, warehouse)
    }
    fn bound(bytes: usize) -> novarocks_fs::FsListingBound {
        novarocks_fs::FsListingBound::try_new(1, bytes, 32 * 1024 * 1024).unwrap()
    }

    #[test]
    fn catalog_host_clone_shares_authorized_inventory_and_only_opts_in_its_clone() {
        let (_directory, host, _warehouse) = listing_host(Arc::new(PaimonFsAuthorizedListing));
        let bounded = host.clone().with_listing_bound(bound(256));
        assert!(Arc::ptr_eq(&host.access, &bounded.access));
        assert!(Arc::ptr_eq(&host.warehouse, &bounded.warehouse));
        assert!(host.listing_bound.is_none());
        assert!(bounded.listing_bound.is_some());
    }

    #[tokio::test]
    async fn catalog_bounded_backend_never_falls_back_to_generic_listing() {
        let listing = Arc::new(GenericOnlyListing::default());
        let (_directory, host, warehouse) = listing_host(listing.clone());
        let bounded = host.clone().with_listing_bound(bound(256));
        let error = match bounded.list(&warehouse, false).await {
            Ok(_) => panic!("unsupported bound accepted"),
            Err(error) => error,
        };
        assert_eq!(
            super::super::catalog::map_sdk_error(error).kind(),
            ConnectorErrorKind::Unsupported
        );
        assert_eq!(listing.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        drop(host.list(&warehouse, false).await.unwrap());
        assert_eq!(listing.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn catalog_host_admits_borrowed_prefix_before_location_copy_or_backend() {
        let listing = Arc::new(GenericOnlyListing::default());
        let (_directory, host, warehouse) = listing_host(listing.clone());
        let path = format!("{warehouse}/{}", "x".repeat(128));
        for (bound, prefix) in [
            (bound(64), path.as_str()),
            (
                novarocks_fs::FsListingBound::try_new(1, 256, 64).unwrap(),
                warehouse.as_str(),
            ),
        ] {
            let bounded = host.clone().with_listing_bound(bound);
            let error = match bounded.list(prefix, false).await {
                Ok(_) => panic!("source copies accepted before their bound"),
                Err(error) => error,
            };
            assert_eq!(
                super::super::catalog::map_sdk_error(error).kind(),
                ConnectorErrorKind::ResourceExhausted
            );
        }
        assert_eq!(listing.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn catalog_host_refuses_long_uri_without_tightening_generic_or_be_host() {
        use futures::StreamExt;
        let (_directory, host, warehouse) = listing_host(Arc::new(PaimonFsAuthorizedListing));
        let file = std::path::Path::new(&warehouse).join("x".repeat(100));
        std::fs::write(&file, b"data").unwrap();
        let file = file.to_string_lossy().into_owned();
        let generic = host
            .list(&warehouse, false)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        // Local OpenDAL may also yield the listed directory itself. Preserve
        // that behavior and verify that the complete long file remains visible.
        assert!(
            generic
                .iter()
                .any(|entry| !entry.is_dir && entry.path == file)
        );
        let bounded = host.clone().with_listing_bound(bound(warehouse.len() + 8));
        let mut stream = bounded.list(&warehouse, false).await.unwrap();
        let error = loop {
            match stream.next().await.expect("long file must cause a refusal") {
                Ok(_) => {}
                Err(error) => break error,
            }
        };
        assert_eq!(
            super::super::catalog::map_sdk_error(error).kind(),
            ConnectorErrorKind::ResourceExhausted
        );
        assert!(stream.next().await.is_none());
        assert!(host.listing_bound.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bounded_catalog_probe_is_fresh_and_never_grows_scan_size_cache() {
        let (_directory, host, warehouse) = listing_host(Arc::new(PaimonFsAuthorizedListing));
        let path = format!("{warehouse}/schema-0");
        std::fs::write(&path, b"old").unwrap();
        // An existing frozen scan fact remains intact, but metadata probes do
        // not consult it or add new keys to this shared scan cache.
        host.know_object_size(&path, 3).unwrap();
        let bounded = host.clone().with_listing_bound(bound(256));
        assert_eq!(bounded.stat(&path).await.unwrap().size, 3);
        std::fs::write(&path, b"replacement").unwrap();
        assert_eq!(bounded.stat(&path).await.unwrap().size, 11);
        std::fs::remove_file(&path).unwrap();
        assert!(!bounded.exists(&path).await.unwrap());
        let other = format!("{warehouse}/schema-7");
        std::fs::write(&other, b"next").unwrap();
        assert_eq!(bounded.stat(&other).await.unwrap().size, 4);
        assert_eq!(host.sizes.known.lock().unwrap().len(), 1);
        assert_eq!(host.remembered_size(&path).unwrap(), Some(3));
        assert_eq!(host.remembered_size(&other).unwrap(), None);
        assert_eq!(bounded.size_probes(), 4);
    }

    #[tokio::test]
    async fn bounded_catalog_probe_refuses_long_schema_and_frozen_read_size_before_io() {
        let (_directory, host, warehouse) = listing_host(Arc::new(PaimonFsAuthorizedListing));
        let bounded = host.with_listing_bound(bound(warehouse.len() + 16));
        let path = format!("{warehouse}/{}/schema-0", "x".repeat(128));
        let error = bounded.stat(&path).await.unwrap_err();
        assert_eq!(
            super::super::catalog::map_sdk_error(error).kind(),
            ConnectorErrorKind::ResourceExhausted
        );
        let path = format!("{warehouse}/schema-0");
        assert_eq!(
            bounded
                .object_size(&path, Some(1))
                .await
                .unwrap_err()
                .kind(),
            FileErrorKind::Invalid
        );
        assert_eq!(bounded.size_probes(), 0);
        assert!(bounded.sizes.known.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn bounded_catalog_probe_keeps_cancel_and_deadline_kinds() {
        let (_directory, host, warehouse) = listing_host(Arc::new(PaimonFsAuthorizedListing));
        let path = format!("{warehouse}/schema-0");
        let mut cancelled = host.clone().with_listing_bound(bound(256));
        cancelled.cancellation = FileCancellation::new();
        cancelled.cancellation.cancel();
        let mut expired = host.with_listing_bound(bound(256));
        expired.cancellation = FileCancellation::new().with_deadline(Some(
            std::time::Instant::now() - std::time::Duration::from_secs(1),
        ));
        for (host, kind) in [
            (cancelled, ConnectorErrorKind::Cancelled),
            (expired, ConnectorErrorKind::DeadlineExceeded),
        ] {
            let error = host.exists(&path).await.unwrap_err();
            assert_eq!(super::super::catalog::map_sdk_error(error).kind(), kind);
            assert_eq!(host.size_probes(), 0);
            assert!(host.sizes.known.lock().unwrap().is_empty());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bounded_catalog_probe_rejects_be_range_binding_in_either_order() {
        let (_directory, host, warehouse) = listing_host(Arc::new(PaimonFsAuthorizedListing));
        let handle = tokio::runtime::Handle::current();
        let service = FileRangeService::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(4).unwrap(),
            Arc::new(TokioFileTaskSpawner::new(handle.clone())),
            handle,
        );
        let range = service.bind(
            FileRangeScope::try_new(1, 0, 1, 2, 0, 3).unwrap(),
            ConnectorSourceOperations::new(),
        );
        for bounded in [
            host.clone()
                .with_listing_bound(bound(256))
                .with_range_binding(range.clone()),
            host.with_range_binding(range)
                .with_listing_bound(bound(256)),
        ] {
            let path = format!("{warehouse}/schema-0");
            let error = bounded.exists(&path).await.unwrap_err();
            assert_eq!(
                super::super::catalog::map_sdk_error(error).kind(),
                ConnectorErrorKind::InvalidRequest
            );
            let error = match bounded.list(&warehouse, false).await {
                Ok(_) => panic!("BE range owner bypass accepted"),
                Err(error) => error,
            };
            assert_eq!(
                super::super::catalog::map_sdk_error(error).kind(),
                ConnectorErrorKind::InvalidRequest
            );
            assert_eq!(bounded.size_probes(), 0);
            assert!(bounded.sizes.known.lock().unwrap().is_empty());
        }
    }

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
            let handle = tokio::runtime::Handle::current();
            Self::with_spawner(files, Arc::new(TokioFileTaskSpawner::new(handle)))
        }

        fn with_spawner(files: &[(&str, &[u8])], spawner: Arc<dyn FileTaskSpawner>) -> Self {
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
            let service = FileRangeService::new(
                NonZeroUsize::new(2).unwrap(),
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(4).unwrap(),
                spawner,
                tokio::runtime::Handle::current(),
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

    /// Holds every spawned file task until the test lets it run.
    struct GatedSpawner {
        gate: Arc<tokio::sync::Semaphore>,
        started: std::sync::atomic::AtomicUsize,
    }

    impl GatedSpawner {
        fn closed() -> Arc<Self> {
            Arc::new(Self {
                gate: Arc::new(tokio::sync::Semaphore::new(0)),
                started: std::sync::atomic::AtomicUsize::new(0),
            })
        }

        fn started(&self) -> usize {
            self.started.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl FileTaskSpawner for GatedSpawner {
        fn spawn(&self, task: FileTaskFuture) -> FileResult<FileTask> {
            self.started
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let gate = Arc::clone(&self.gate);
            Ok(FileTask::new(tokio::spawn(async move {
                gate.acquire_owned().await.expect("gate").forget();
                task.await;
            })))
        }

        fn spawn_detached_blocking(&self, job: Box<dyn FnOnce() + Send + 'static>) {
            tokio::task::spawn_blocking(job);
        }
    }

    /// A ledger that only counts what is currently reserved.
    #[derive(Default)]
    struct CountingLedger {
        retained: Arc<std::sync::atomic::AtomicU64>,
    }

    impl CountingLedger {
        fn retained(&self) -> u64 {
            self.retained.load(std::sync::atomic::Ordering::Acquire)
        }
    }

    impl novarocks_spi::connector::ConnectorResourceLedger for CountingLedger {
        fn checkpoint(
            &self,
        ) -> Result<
            novarocks_spi::connector::ConnectorResourceCheckpoint,
            novarocks_spi::connector::ConnectorError,
        > {
            Ok(novarocks_spi::connector::ConnectorResourceCheckpoint::new(
                1,
            ))
        }

        fn try_reserve(
            &self,
            _class: novarocks_spi::connector::ConnectorResourceClass,
            bytes: u64,
        ) -> Result<
            Box<dyn novarocks_spi::connector::ConnectorResourceLease>,
            novarocks_spi::connector::ConnectorError,
        > {
            self.retained
                .fetch_add(bytes, std::sync::atomic::Ordering::AcqRel);
            Ok(Box::new(CountedLease {
                retained: Arc::clone(&self.retained),
                bytes,
            }))
        }
    }

    struct CountedLease {
        retained: Arc<std::sync::atomic::AtomicU64>,
        bytes: u64,
    }

    impl novarocks_spi::connector::ConnectorResourceLease for CountedLease {
        fn bytes(&self) -> u64 {
            self.bytes
        }

        fn try_grow(
            &mut self,
            additional: u64,
        ) -> Result<(), novarocks_spi::connector::ConnectorError> {
            self.retained
                .fetch_add(additional, std::sync::atomic::Ordering::AcqRel);
            self.bytes += additional;
            Ok(())
        }

        fn shrink_to(
            &mut self,
            bytes: u64,
        ) -> Result<(), novarocks_spi::connector::ConnectorError> {
            self.retained
                .fetch_sub(self.bytes - bytes, std::sync::atomic::Ordering::AcqRel);
            self.bytes = bytes;
            Ok(())
        }
    }

    impl Drop for CountedLease {
        fn drop(&mut self) {
            self.retained
                .fetch_sub(self.bytes, std::sync::atomic::Ordering::AcqRel);
        }
    }

    /// The execution-side host FileIO over `file_io`, charging `ledger`.
    fn charged(
        file_io: &PaimonHostFileIo,
        ledger: &Arc<CountingLedger>,
    ) -> PaimonChargedHostFileIo {
        let resources = crate::resources::PaimonExecutionResources::new(
            crate::resources::PaimonRequestControl::new(
                novarocks_spi::connector::ConnectorStopOwner::new().view(),
                std::time::Instant::now() + std::time::Duration::from_secs(60),
            ),
            novarocks_spi::connector::ConnectorExecutionResources::from_admitted_ledger(
                Arc::clone(ledger) as Arc<dyn novarocks_spi::connector::ConnectorResourceLedger>,
            ),
        );
        let sdk_resources = Arc::new(crate::sdk_control::PaimonSdkExecutionResources::new(
            resources.clone(),
            novarocks_spi::connector::read_stack::ConnectorPollBudget::new(),
        ));
        PaimonChargedHostFileIo::new(file_io.clone(), resources, sdk_resources)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_read_dropped_mid_flight_stays_charged_until_its_physical_exit() {
        let spawner = GatedSpawner::closed();
        let warehouse = LocalWarehouse::with_spawner(
            &[("bucket-0/data-0.parquet", b"paimon-bytes")],
            spawner.clone(),
        );
        let data = warehouse.path("bucket-0/data-0.parquet");
        warehouse
            .file_io
            .know_object_size(&data, 12)
            .expect("frozen size");
        let ledger = Arc::new(CountingLedger::default());
        let file_io = charged(&warehouse.file_io, &ledger);
        let read = tokio::spawn({
            let data = data.clone();
            async move { file_io.read(&data, 0..6, Some(12)).await }
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while spawner.started() == 0 {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the physical read started");
        assert_eq!(ledger.retained(), 6);

        read.abort();
        assert!(
            read.await.is_err(),
            "the read future was dropped mid-flight"
        );
        assert_eq!(
            ledger.retained(),
            6,
            "the stopped read's task can still hold its bytes"
        );
        spawner.gate.add_permits(1);
        warehouse.service.drain().await.expect("the read exits");
        assert_eq!(ledger.retained(), 0, "the charge is returned with the exit");
        warehouse.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_successful_read_stays_charged_until_its_last_bytes_are_dropped() {
        let warehouse = LocalWarehouse::with_files(&[("bucket-0/data-0.parquet", b"paimon-bytes")]);
        let data = warehouse.path("bucket-0/data-0.parquet");
        warehouse
            .file_io
            .know_object_size(&data, 12)
            .expect("frozen size");
        let ledger = Arc::new(CountingLedger::default());
        let file_io = charged(&warehouse.file_io, &ledger);
        let bytes = file_io.read(&data, 0..6, Some(12)).await.expect("GET");
        warehouse.service.drain().await.expect("the read exited");
        assert_eq!(&bytes[..], b"paimon");
        assert_eq!(ledger.retained(), 6, "the bytes outlive the physical read");
        let clone = bytes.clone();
        drop(bytes);
        assert_eq!(ledger.retained(), 6);
        drop(clone);
        assert_eq!(ledger.retained(), 0);
        warehouse.finish().await;
    }
}
