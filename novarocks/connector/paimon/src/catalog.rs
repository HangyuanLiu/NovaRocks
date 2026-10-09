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

use std::sync::Arc;

use novarocks_spi::connector::read_stack::SchemaTableName;
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind, ConnectorListingBound};
use paimon::catalog::Identifier;
use paimon::io::{FileIO, ReadOnlyFileIO};
use paimon::{Catalog, CatalogOptions, FileSystemCatalog, Options};

use crate::io::PaimonHostFileIo;
use crate::metadata::{PaimonFrozenRead, PaimonFrozenReadRecipe, freeze_table, rebind_table};
use crate::resources::PaimonRequestControl;
use crate::sdk_control::PaimonSdkReadControl;

#[path = "catalog_listing.rs"]
mod listing;

#[path = "listing_admission.rs"]
pub(crate) mod listing_admission;
use listing_admission::ListingAdmission;

/// FE-owned catalog entries. The vector owns its elements until materialized or dropped.
pub struct PaimonCatalogEntries {
    entries: Vec<String>,
}

impl PaimonCatalogEntries {
    pub fn entries(&self) -> &[String] {
        &self.entries
    }

    pub fn map<T>(self, transform: impl FnOnce(Vec<String>) -> T) -> T {
        transform(self.entries)
    }
}

impl std::fmt::Debug for PaimonCatalogEntries {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PaimonCatalogEntries")
            .field("entries", &self.entries)
            .finish_non_exhaustive()
    }
}

/// FE-only, read-only filesystem catalog. The raw SDK catalog is never exposed,
/// so callers cannot reach its mutation methods.
#[derive(Clone)]
pub struct PaimonFileSystemCatalog {
    inner: FileSystemCatalog,
    host_io: CatalogListingHost,
    control: PaimonRequestControl,
    listing_admission: ListingAdmission,
}

/// Production retains the concrete admitted host so a catalog listing can
/// select its bounded filesystem seam explicitly. Fixture IO is not a product
/// escape hatch and is compiled only for the source oracle tests.
#[derive(Clone)]
enum CatalogListingHost {
    Host(Arc<PaimonHostFileIo>),
    #[cfg(test)]
    Fixture(Arc<dyn ReadOnlyFileIO>),
}

impl CatalogListingHost {
    fn bounded_io(
        &self,
        bound: ConnectorListingBound,
    ) -> Result<Arc<dyn ReadOnlyFileIO>, ConnectorError> {
        match self {
            Self::Host(host) => {
                let fs_bound = novarocks_fs::FsListingBound::try_new(
                    bound.page_entries,
                    ConnectorListingBound::V1.name_bytes,
                    32 * 1024 * 1024,
                )
                .map_err(|_| invalid("Paimon filesystem listing bound is invalid"))?;
                Ok(Arc::new(host.as_ref().clone().with_listing_bound(fs_bound)))
            }
            #[cfg(test)]
            Self::Fixture(host) => Ok(Arc::clone(host)),
        }
    }
}

impl PaimonFileSystemCatalog {
    pub fn try_new(
        warehouse: impl AsRef<str>,
        host_io: PaimonHostFileIo,
        control: PaimonRequestControl,
    ) -> Result<Self, ConnectorError> {
        control.checkpoint()?;
        let warehouse = warehouse.as_ref();
        if warehouse.is_empty() {
            return Err(invalid("Paimon warehouse location must be non-empty"));
        }
        let sdk_control = PaimonSdkReadControl::new(control.clone());
        let host_io = Arc::new(host_io);
        let file_io = FileIO::from_read_only(host_io.clone(), Arc::new(sdk_control));
        let mut options = Options::new();
        options.set(CatalogOptions::WAREHOUSE, warehouse);
        let inner = FileSystemCatalog::with_file_io(options, file_io).map_err(map_sdk_error)?;
        Ok(Self {
            inner,
            host_io: CatalogListingHost::Host(host_io),
            control,
            listing_admission: ListingAdmission::default(),
        })
    }

    pub(crate) fn with_listing_admission(mut self, admission: ListingAdmission) -> Self {
        self.listing_admission = admission;
        self
    }

    pub fn warehouse(&self) -> &str {
        self.inner.warehouse()
    }

    /// List databases through a request-local bounded SDK source. An error
    /// refuses the complete result rather than publishing a truncated listing.
    pub async fn list_databases(
        &self,
        bound: ConnectorListingBound,
    ) -> Result<PaimonCatalogEntries, ConnectorError> {
        bound.validate()?;
        self.control.checkpoint()?;
        let _permit = self.listing_admission.acquire(&self.control).await?;
        let inner = self.listing_catalog(bound, None)?;
        let entries = self
            .control
            .until(inner.list_databases_plain())
            .await?
            .map_err(map_sdk_error)?;
        self.retain_listing(entries, bound)
    }

    /// List tables with the SDK's unchanged schema-based existence checks.
    /// Root and nested schema listings share one bounded source workspace.
    pub async fn list_tables(
        &self,
        database: &str,
        bound: ConnectorListingBound,
    ) -> Result<PaimonCatalogEntries, ConnectorError> {
        bound.validate()?;
        self.control.checkpoint()?;
        let _permit = self.listing_admission.acquire(&self.control).await?;
        let inner = self.listing_catalog(bound, Some(database))?;
        let entries = self
            .control
            .until(inner.list_tables_plain(database))
            .await?
            .map_err(map_sdk_error)?;
        self.retain_listing(entries, bound)
    }

    pub(crate) async fn namespace_exists(&self, namespace: &str) -> Result<bool, ConnectorError> {
        let entries = self.list_databases(ConnectorListingBound::V1).await?;
        Ok(entries.entries().iter().any(|name| name == namespace))
    }

    pub(crate) async fn table_exists(
        &self,
        namespace: &str,
        table: &str,
    ) -> Result<bool, ConnectorError> {
        let entries = self
            .list_tables(namespace, ConnectorListingBound::V1)
            .await?;
        Ok(entries.entries().iter().any(|name| name == table))
    }

    /// Load and freeze one table. Snapshot discovery happens exactly once in
    /// `freeze_table`; later split planning uses only its exact SDK table copy.
    pub async fn prepare_read(
        &self,
        name: &SchemaTableName,
    ) -> Result<Arc<PaimonFrozenRead>, ConnectorError> {
        self.control.checkpoint()?;
        let identifier = Identifier::new(name.schema_name(), name.table_name());
        let table = self
            .inner
            .get_table(&identifier)
            .await
            .map_err(map_sdk_error)?;
        freeze_table(table, name.clone(), self.control.clone())
            .await
            .map(Arc::new)
    }

    /// Rebind one already-frozen semantic recipe to this request's FileIO and
    /// operation control. No catalog-current or latest-snapshot lookup occurs.
    pub(crate) fn rebind_read(
        &self,
        recipe: &PaimonFrozenReadRecipe,
    ) -> Result<Arc<PaimonFrozenRead>, ConnectorError> {
        rebind_table(self.inner.file_io().clone(), recipe, self.control.clone()).map(Arc::new)
    }

    fn listing_catalog(
        &self,
        bound: ConnectorListingBound,
        database: Option<&str>,
    ) -> Result<FileSystemCatalog, ConnectorError> {
        // Preflight before cloning the warehouse into options and SDK state.
        let host = listing::BoundedListingIo::new(
            self.host_io.bounded_io(bound)?,
            self.control.clone(),
            self.warehouse(),
            database,
            bound,
        )?;
        let file_io = FileIO::from_read_only(
            Arc::new(host),
            Arc::new(PaimonSdkReadControl::new(self.control.clone())),
        );
        let mut options = Options::new();
        options.set(CatalogOptions::WAREHOUSE, self.warehouse());
        FileSystemCatalog::with_file_io(options, file_io).map_err(map_sdk_error)
    }

    fn retain_listing(
        &self,
        entries: Vec<String>,
        bound: ConnectorListingBound,
    ) -> Result<PaimonCatalogEntries, ConnectorError> {
        if entries.iter().any(|entry| entry.is_empty()) {
            return Err(invalid("Paimon catalog entry name is empty"));
        }
        bound.check_complete_listing(&entries)?;
        self.control.checkpoint()?;
        Ok(PaimonCatalogEntries { entries })
    }
}

impl std::fmt::Debug for PaimonFileSystemCatalog {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PaimonFileSystemCatalog")
            .field("warehouse", &self.inner.warehouse())
            .finish_non_exhaustive()
    }
}

pub(crate) fn map_sdk_error(error: paimon::Error) -> ConnectorError {
    if let paimon::Error::UnexpectedError {
        source: Some(source),
        ..
    } = &error
    {
        if let Some(host_error) = source.downcast_ref::<ConnectorError>() {
            return host_error.clone();
        }
        if let Some(file_error) = source.downcast_ref::<novarocks_fs::FileError>() {
            return crate::io::connector_error_from_file_error(file_error);
        }
    }
    let kind = match &error {
        paimon::Error::DatabaseNotExist { .. }
        | paimon::Error::TableNotExist { .. }
        | paimon::Error::ViewNotExist { .. }
        | paimon::Error::FunctionNotExist { .. }
        | paimon::Error::ColumnNotExist { .. } => ConnectorErrorKind::NotFound,
        paimon::Error::Unsupported { .. } | paimon::Error::IoUnsupported { .. } => {
            ConnectorErrorKind::Unsupported
        }
        paimon::Error::DataInvalid { .. }
        | paimon::Error::DataTypeInvalid { .. }
        | paimon::Error::FileIndexFormatInvalid { .. } => ConnectorErrorKind::CorruptData,
        paimon::Error::ConfigInvalid { .. } | paimon::Error::IdentifierInvalid { .. } => {
            ConnectorErrorKind::InvalidRequest
        }
        paimon::Error::IoUnexpected { .. } | paimon::Error::RestApi { .. } => {
            ConnectorErrorKind::Unavailable
        }
        _ => ConnectorErrorKind::Internal,
    };
    ConnectorError::new(kind, format!("Paimon SDK rejected read metadata: {error}"))
}

fn invalid(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use novarocks_spi::connector::ConnectorErrorKind;
    use paimon::io::{FileIO, FileStatus, FileStatusStream, ReadOnlyFileIO};
    use paimon::{CatalogOptions, FileSystemCatalog, Options};

    use super::*;

    #[derive(Debug)]
    struct DatabaseListingIo {
        warehouse: String,
        database_count: usize,
    }

    #[async_trait::async_trait]
    impl ReadOnlyFileIO for DatabaseListingIo {
        async fn stat(&self, _path: &str) -> paimon::Result<FileStatus> {
            Err(paimon::Error::IoUnsupported {
                message: "test backend has no stat path".to_string(),
            })
        }

        async fn exists(&self, _path: &str) -> paimon::Result<bool> {
            Ok(false)
        }

        async fn read(
            &self,
            _path: &str,
            _range: Range<u64>,
            _known_size: Option<u64>,
        ) -> paimon::Result<Bytes> {
            Err(paimon::Error::IoUnsupported {
                message: "test backend has no read path".to_string(),
            })
        }

        async fn list(&self, path: &str, _recursive: bool) -> paimon::Result<FileStatusStream> {
            let count = if path == self.warehouse {
                self.database_count
            } else {
                0
            };
            let warehouse = self.warehouse.clone();
            Ok(Box::pin(futures::stream::iter((0..count).map(
                move |index| {
                    let name = if index == 0 {
                        "db".to_string()
                    } else {
                        format!("db{index}")
                    };
                    Ok(FileStatus {
                        size: 0,
                        is_dir: true,
                        // Match the bounded host's exact URI backing. The row
                        // ceiling oracle must also fit the independent source
                        // workspace; spare capacity has its own refusal tests.
                        path: format!("{warehouse}/{name}.db/")
                            .into_boxed_str()
                            .into_string(),
                        last_modified: None,
                    })
                },
            ))))
        }
    }

    fn catalog(
        cancellation: Arc<novarocks_spi::connector::ConnectorStopOwner>,
        database_count: usize,
    ) -> PaimonFileSystemCatalog {
        let warehouse = "s3://bucket/warehouse";
        let control = PaimonRequestControl::new(
            cancellation.view(),
            Instant::now() + Duration::from_secs(60),
        );
        let host_io: Arc<dyn ReadOnlyFileIO> = Arc::new(DatabaseListingIo {
            warehouse: warehouse.to_string(),
            database_count,
        });
        let file_io = FileIO::from_read_only(
            Arc::clone(&host_io),
            Arc::new(PaimonSdkReadControl::new(control.clone())),
        );
        let mut options = Options::new();
        options.set(CatalogOptions::WAREHOUSE, warehouse);
        let inner = FileSystemCatalog::with_file_io(options, file_io).unwrap();
        PaimonFileSystemCatalog {
            inner,
            host_io: CatalogListingHost::Fixture(host_io),
            control,
            listing_admission: ListingAdmission::default(),
        }
    }

    #[tokio::test]
    async fn fe_listing_is_plain_owned_and_materializes() {
        let catalog = catalog(
            Arc::new(novarocks_spi::connector::ConnectorStopOwner::new()),
            1,
        );
        let entries = catalog
            .list_databases(ConnectorListingBound::V1)
            .await
            .unwrap();
        assert_eq!(entries.entries(), &["db".to_string()]);
        let identities = entries.map(|entries| {
            entries
                .into_iter()
                .map(Arc::<str>::from)
                .collect::<Vec<_>>()
        });
        assert_eq!(identities, vec![Arc::<str>::from("db")]);
    }

    #[tokio::test]
    async fn fe_listing_admits_exactly_the_v1_entry_bound() {
        let catalog = catalog(
            Arc::new(novarocks_spi::connector::ConnectorStopOwner::new()),
            65_536,
        );
        let entries = catalog
            .list_databases(ConnectorListingBound::V1)
            .await
            .unwrap();
        assert_eq!(entries.entries().len(), 65_536);
        assert!(entries.entries().contains(&"db65535".to_string()));
    }

    #[tokio::test]
    async fn fe_listing_over_the_v1_entry_bound_is_refused_not_truncated() {
        let catalog = catalog(
            Arc::new(novarocks_spi::connector::ConnectorStopOwner::new()),
            65_537,
        );
        let error = catalog
            .list_databases(ConnectorListingBound::V1)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
        assert!(error.message().contains("entries bound"), "{error}");
    }

    /// A warehouse on local disk, listed through the host's authorized
    /// filesystem access.
    fn local_catalog(directory: &tempfile::TempDir, databases: &[&str]) -> PaimonFileSystemCatalog {
        let warehouse = directory.path().join("warehouse");
        for database in databases {
            std::fs::create_dir_all(warehouse.join(format!("{database}.db")))
                .expect("database directory");
        }
        let warehouse = warehouse.to_string_lossy().to_string();
        let access = novarocks_fs::FsAccessResolver::new()
            .resolve_location(
                novarocks_spi::connector::StorageAccessDomainId::from_bytes([3; 32]),
                &warehouse,
                None,
            )
            .expect("local access");
        let host_io = PaimonHostFileIo::try_new(
            access,
            &warehouse,
            novarocks_fs::FileCancellation::new(),
            Arc::new(crate::io::PaimonFsAuthorizedListing),
        )
        .expect("host file io");
        let control = PaimonRequestControl::new(
            Arc::new(novarocks_spi::connector::ConnectorStopOwner::new()).view(),
            Instant::now() + Duration::from_secs(60),
        );
        PaimonFileSystemCatalog::try_new(&warehouse, host_io, control).expect("catalog")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_filesystem_listing_over_its_bound_is_refused_whole() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let catalog = local_catalog(&directory, &["sales", "ops", "hr"]);
        let exact = ConnectorListingBound {
            entries: 3,
            total_name_bytes: 10,
            ..ConnectorListingBound::V1
        };
        let mut names = catalog
            .list_databases(exact)
            .await
            .expect("listing at its exact bound")
            .entries()
            .to_vec();
        names.sort();
        assert_eq!(names, ["hr", "ops", "sales"]);

        for (bound, exceeded) in [
            (
                ConnectorListingBound {
                    entries: 2,
                    ..exact
                },
                "entries",
            ),
            (
                ConnectorListingBound {
                    total_name_bytes: 9,
                    ..exact
                },
                "total_name_bytes",
            ),
            (
                ConnectorListingBound {
                    name_bytes: 4,
                    ..exact
                },
                "name_bytes",
            ),
        ] {
            let error = catalog.list_databases(bound).await.unwrap_err();
            assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
            assert!(
                error.message().contains(&format!("{exceeded} bound")),
                "{error}"
            );
        }
    }

    #[tokio::test]
    async fn fe_listing_still_observes_request_cancellation() {
        let cancellation = Arc::new(novarocks_spi::connector::ConnectorStopOwner::new());
        let catalog = catalog(Arc::clone(&cancellation), 1);
        cancellation.request_stop();
        let error = catalog
            .list_databases(ConnectorListingBound::V1)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::Cancelled);
    }
    #[derive(Debug)]
    struct RefusedListingIo;
    #[async_trait::async_trait]
    impl ReadOnlyFileIO for RefusedListingIo {
        async fn stat(&self, _path: &str) -> paimon::Result<FileStatus> {
            unreachable!("existence listing does not use stat")
        }
        async fn exists(&self, _path: &str) -> paimon::Result<bool> {
            Ok(true)
        }
        async fn read(
            &self,
            _path: &str,
            _range: Range<u64>,
            _known_size: Option<u64>,
        ) -> paimon::Result<Bytes> {
            unreachable!("listing failure precedes read")
        }
        async fn list(&self, _path: &str, _recursive: bool) -> paimon::Result<FileStatusStream> {
            Err(paimon::Error::UnexpectedError {
                message: "listing refused".to_owned(),
                source: Some(Box::new(ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "external listing limit exceeded",
                ))),
            })
        }
    }

    #[tokio::test]
    async fn existence_propagates_listing_failure_instead_of_false() {
        let mut catalog = catalog(
            Arc::new(novarocks_spi::connector::ConnectorStopOwner::new()),
            0,
        );
        catalog.host_io = CatalogListingHost::Fixture(Arc::new(RefusedListingIo));
        assert_eq!(
            catalog.namespace_exists("db").await.unwrap_err().kind(),
            ConnectorErrorKind::ResourceExhausted
        );
        assert_eq!(
            catalog
                .table_exists("db", "table")
                .await
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::ResourceExhausted
        );
    }

    #[derive(Debug)]
    struct PendingListingIo(Arc<std::sync::atomic::AtomicUsize>, bool);
    #[async_trait::async_trait]
    impl ReadOnlyFileIO for PendingListingIo {
        async fn stat(&self, _path: &str) -> paimon::Result<FileStatus> {
            unreachable!()
        }
        async fn exists(&self, _path: &str) -> paimon::Result<bool> {
            Ok(true)
        }
        async fn read(
            &self,
            _path: &str,
            _range: Range<u64>,
            _known_size: Option<u64>,
        ) -> paimon::Result<Bytes> {
            unreachable!()
        }
        async fn list(&self, _path: &str, _recursive: bool) -> paimon::Result<FileStatusStream> {
            struct Guard(Arc<std::sync::atomic::AtomicUsize>);
            impl Drop for Guard {
                fn drop(&mut self) {
                    self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
            }
            let guard = Guard(self.0.clone());
            if self.1 {
                Ok(Box::pin(futures::stream::unfold(
                    guard,
                    |guard| async move {
                        let item =
                            futures::future::pending::<Option<paimon::Result<FileStatus>>>().await;
                        item.map(|item| (item, guard))
                    },
                )))
            } else {
                let _guard = guard;
                futures::future::pending().await
            }
        }
    }

    #[tokio::test]
    async fn pending_catalog_listing_deadline_drops_source_and_returns_admission() {
        let stop = Arc::new(novarocks_spi::connector::ConnectorStopOwner::new());
        let mut catalog = catalog(stop.clone(), 0);
        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        catalog.host_io =
            CatalogListingHost::Fixture(Arc::new(PendingListingIo(drops.clone(), false)));
        catalog.control =
            PaimonRequestControl::new(stop.view(), Instant::now() + Duration::from_millis(20));
        assert_eq!(
            catalog
                .list_databases(ConnectorListingBound::V1)
                .await
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::DeadlineExceeded
        );
        assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 1);
        // Reuse the same gate after the pending SDK operation has been dropped.
        let fresh =
            PaimonRequestControl::new(stop.view(), Instant::now() + Duration::from_secs(10));
        let mut permits = Vec::new();
        for _ in 0..listing_admission::LISTING_CONCURRENCY {
            permits.push(catalog.listing_admission.acquire(&fresh).await.unwrap());
        }
    }
    #[tokio::test]
    async fn pending_catalog_stream_stop_drops_source_and_returns_admission() {
        let stop = Arc::new(novarocks_spi::connector::ConnectorStopOwner::new());
        let mut catalog = catalog(stop.clone(), 0);
        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        catalog.host_io =
            CatalogListingHost::Fixture(Arc::new(PendingListingIo(drops.clone(), true)));
        let wait = catalog.list_databases(ConnectorListingBound::V1);
        tokio::pin!(wait);
        assert!(futures::poll!(&mut wait).is_pending());
        stop.request_stop();
        assert_eq!(
            wait.await.unwrap_err().kind(),
            ConnectorErrorKind::Cancelled
        );
        assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 1);
        let fresh = PaimonRequestControl::new(
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            Instant::now() + Duration::from_secs(10),
        );
        let mut permits = Vec::new();
        for _ in 0..listing_admission::LISTING_CONCURRENCY {
            permits.push(catalog.listing_admission.acquire(&fresh).await.unwrap());
        }
    }
}
