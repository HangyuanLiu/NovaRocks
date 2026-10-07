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

use std::mem::size_of;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::StreamExt;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorListingBound, ConnectorListingBudget,
};
use paimon::io::{FileStatus, FileStatusStream, ReadOnlyFileIO};

use crate::resources::PaimonRequestControl;

const WORKSPACE_BYTES: usize = 32 * 1024 * 1024;
// Includes the explicit bounded FS URI/context transient allowance. This is
// part of the same frozen 32 MiB call workspace, not another capacity grant.
const FIXED_BYTES: usize = 2 * 1024 * 1024;

/// A catalog-call source bound, not a BE reader reservation or a process ledger.
///
/// The production host checks URI construction and fresh metadata probes at
/// the filesystem seam, inside the same workspace's fixed auxiliary allowance.
/// OpenDAL's page receive/XML decode remain outside this source proof.
#[derive(Debug)]
pub(super) struct BoundedListingIo {
    host: Arc<dyn ReadOnlyFileIO>,
    control: PaimonRequestControl,
    database_root: Option<String>,
    budget: Arc<Mutex<SourceBudget>>,
}

#[derive(Debug)]
struct SourceBudget {
    workspace_limit: usize,
    workspace_bytes: usize,
    databases: ConnectorListingBudget,
}

impl SourceBudget {
    fn new(
        warehouse: &str,
        database: Option<&str>,
        bound: ConnectorListingBound,
        workspace_limit: usize,
    ) -> Result<Self, ConnectorError> {
        let databases = ConnectorListingBudget::new(bound)?;
        // Options, SDK warehouse/path copies, the wrapper root, identifiers and
        // the active stat/probe paths. Check before making any of these copies.
        let workspace_bytes = warehouse
            .len()
            .checked_add(database.map_or(0, str::len))
            .and_then(|bytes| bytes.checked_mul(8))
            .and_then(|bytes| bytes.checked_add(FIXED_BYTES))
            .ok_or_else(exhausted)?;
        if workspace_bytes > workspace_limit || workspace_limit > WORKSPACE_BYTES {
            return Err(exhausted());
        }
        Ok(Self {
            workspace_limit,
            workspace_bytes,
            databases,
        })
    }

    fn admit(&mut self, status: &FileStatus, database_root: bool) -> Result<(), ConnectorError> {
        // Pinned SDK 0.3.0 creates statuses -> dirs -> names, or statuses -> IDs.
        // Vec amortized growth has capacity <= max(4, 2*n); old+new <= 3*n
        // (the initial four slots are covered by FIXED_BYTES). At either collect
        // boundary, retained predecessor + growth is <= 5*n header slots.
        // Six largest-header slots also cover the yielded item and active probe.
        // Four path capacities cover original paths and basename/name copies.
        // Four extra basename lengths cover Identifier.object, table_path and
        // the simultaneously live schema_directory/schema_path format buffers:
        // appending the fixed suffix can double each formatting Vec. Warehouse
        // and database portions of the active probe are covered at construction.
        // Count every physical entry, including filtered
        // files and every nested schema listing. Charges never reset within a
        // call: this conservatively covers root dirs retained during probes.
        // schema IDs use sort_unstable: no heap scratch; its bounded recursion
        // and the sequential async probe state fit the fixed allowance. Rust
        // 1.92 alloc/raw_vec::grow_amortized supplies the doubling bound; SDK
        // filter_map collectors have a zero lower size hint (no large reserve).
        let header = size_of::<FileStatus>()
            .max(size_of::<String>())
            .max(size_of::<i64>());
        let basename = basename(&status.path);
        let bytes = status
            .path
            .capacity()
            .checked_mul(4)
            .and_then(|bytes| {
                basename
                    .len()
                    .checked_mul(4)
                    .and_then(|name| bytes.checked_add(name))
            })
            .and_then(|bytes| bytes.checked_add(6 * header))
            .and_then(|bytes| bytes.checked_add(self.workspace_bytes))
            .ok_or_else(exhausted)?;
        if bytes > self.workspace_limit {
            return Err(exhausted());
        }
        if database_root && status.is_dir {
            // Match the SDK's borrowed basename + trailing-slash + .db filter.
            // Do not count ignored physical entries as logical database names.
            if status.path.ends_with('/') {
                if let Some(name) = basename.strip_suffix(".db") {
                    self.databases.admit_names(std::iter::once(name))?;
                }
            }
        }
        self.workspace_bytes = bytes;
        Ok(())
    }
}

impl BoundedListingIo {
    pub(super) fn new(
        host: Arc<dyn ReadOnlyFileIO>,
        control: PaimonRequestControl,
        warehouse: &str,
        database: Option<&str>,
        bound: ConnectorListingBound,
    ) -> Result<Self, ConnectorError> {
        let budget = SourceBudget::new(warehouse, database, bound, WORKSPACE_BYTES)?;
        Ok(Self {
            host,
            control,
            database_root: database.is_none().then(|| warehouse.to_owned()),
            budget: Arc::new(Mutex::new(budget)),
        })
    }
}

#[async_trait::async_trait]
impl ReadOnlyFileIO for BoundedListingIo {
    async fn stat(&self, path: &str) -> paimon::Result<FileStatus> {
        self.host.stat(path).await
    }
    async fn exists(&self, path: &str) -> paimon::Result<bool> {
        self.host.exists(path).await
    }
    async fn read(
        &self,
        path: &str,
        range: Range<u64>,
        known_size: Option<u64>,
    ) -> paimon::Result<Bytes> {
        self.host.read(path, range, known_size).await
    }
    async fn list(&self, path: &str, recursive: bool) -> paimon::Result<FileStatusStream> {
        self.control.checkpoint().map_err(sdk_error)?;
        let stream = self.host.list(path, recursive).await?;
        let database_root = self.database_root.as_deref() == Some(path);
        let initial = Some((
            stream,
            Arc::clone(&self.budget),
            self.control.clone(),
            database_root,
        ));
        Ok(Box::pin(futures::stream::unfold(
            initial,
            |state| async move {
                let (mut stream, budget, control, database_root) = state?;
                if let Err(error) = control.checkpoint() {
                    return Some((Err(sdk_error(error)), None));
                }
                let status = match stream.next().await? {
                    Ok(status) => status,
                    Err(error) => return Some((Err(error), None)),
                };
                let admitted = control.checkpoint().and_then(|()| {
                    let mut budget = budget.lock().map_err(|_| {
                        ConnectorError::new(
                            ConnectorErrorKind::Internal,
                            "Paimon listing source budget lock poisoned",
                        )
                    })?;
                    budget.admit(&status, database_root)
                });
                match admitted {
                    Ok(()) => Some((Ok(status), Some((stream, budget, control, database_root)))),
                    Err(error) => Some((Err(sdk_error(error)), None)),
                }
            },
        )))
    }
}

fn basename(path: &str) -> &str {
    // Equivalent to SDK get_basename followed by the directory slash removal.
    let path = path.strip_suffix('/').unwrap_or(path);
    path.rsplit('/').next().unwrap_or(path)
}

fn exhausted() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        "Paimon catalog listing source workspace bound exceeded",
    )
}

fn sdk_error(error: ConnectorError) -> paimon::Error {
    paimon::Error::UnexpectedError {
        message: "Paimon bounded catalog listing refused its source".to_owned(),
        source: Some(Box::new(error)),
    }
}

#[cfg(test)]
mod tests {
    use super::super::{PaimonFileSystemCatalog, map_sdk_error};
    use super::*;
    use paimon::io::FileIO;
    use paimon::{CatalogOptions, FileSystemCatalog, Options};
    use std::collections::{HashMap, HashSet};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    const ROOT: &str = "s3://bucket/warehouse";

    #[derive(Clone, Debug)]
    struct Seed {
        path: &'static str,
        directory: bool,
        capacity: usize,
    }

    #[derive(Debug)]
    struct TestIo {
        listings: HashMap<&'static str, Arc<Vec<Seed>>>,
        exists: HashSet<&'static str>,
        polls: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
        calls: Arc<AtomicUsize>,
    }

    struct DropMark(Arc<AtomicUsize>);
    impl Drop for DropMark {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl ReadOnlyFileIO for TestIo {
        async fn stat(&self, _path: &str) -> paimon::Result<FileStatus> {
            panic!("listing must not stat")
        }
        async fn exists(&self, path: &str) -> paimon::Result<bool> {
            Ok(self.exists.contains(path))
        }
        async fn read(
            &self,
            _path: &str,
            _range: Range<u64>,
            _known_size: Option<u64>,
        ) -> paimon::Result<Bytes> {
            panic!("listing must not deserialize table schema")
        }
        async fn list(&self, path: &str, _recursive: bool) -> paimon::Result<FileStatusStream> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let entries = self.listings.get(path).cloned().unwrap_or_default();
            let polls = Arc::clone(&self.polls);
            let mark = DropMark(Arc::clone(&self.drops));
            Ok(Box::pin(futures::stream::unfold(
                (0, entries, polls, mark),
                |(index, entries, polls, mark)| async move {
                    let seed = entries.get(index)?;
                    polls.fetch_add(1, Ordering::SeqCst);
                    let mut path = String::with_capacity(seed.capacity.max(seed.path.len()));
                    path.push_str(seed.path);
                    let status = FileStatus {
                        path,
                        is_dir: seed.directory,
                        size: 0,
                        last_modified: None,
                    };
                    Some((Ok(status), (index + 1, entries, polls, mark)))
                },
            )))
        }
    }

    fn seed(path: &'static str, directory: bool) -> Seed {
        Seed {
            path,
            directory,
            capacity: path.len(),
        }
    }
    fn host(listings: Vec<(&'static str, Vec<Seed>)>, exists: &[&'static str]) -> Arc<TestIo> {
        Arc::new(TestIo {
            listings: listings
                .into_iter()
                .map(|(path, seeds)| (path, Arc::new(seeds)))
                .collect(),
            exists: exists.iter().copied().collect(),
            polls: Arc::new(AtomicUsize::new(0)),
            drops: Arc::new(AtomicUsize::new(0)),
            calls: Arc::new(AtomicUsize::new(0)),
        })
    }
    fn control() -> PaimonRequestControl {
        PaimonRequestControl::new(
            Arc::new(novarocks_spi::connector::ConnectorStopOwner::new()).view(),
            Instant::now() + Duration::from_secs(60),
        )
    }
    fn sdk(host: Arc<dyn ReadOnlyFileIO>, control: PaimonRequestControl) -> FileSystemCatalog {
        let file_io = FileIO::from_read_only(
            host,
            Arc::new(crate::sdk_control::PaimonSdkReadControl::new(control)),
        );
        let mut options = Options::new();
        options.set(CatalogOptions::WAREHOUSE, ROOT);
        FileSystemCatalog::with_file_io(options, file_io).unwrap()
    }
    fn catalog(host: Arc<dyn ReadOnlyFileIO>) -> PaimonFileSystemCatalog {
        let control = control();
        PaimonFileSystemCatalog {
            inner: sdk(Arc::clone(&host), control.clone()),
            host_io: super::super::CatalogListingHost::Fixture(host),
            control,
        }
    }
    fn cost(seed: &Seed) -> usize {
        4 * seed.capacity.max(seed.path.len())
            + 4 * basename(seed.path).len()
            + 6 * size_of::<FileStatus>()
    }

    #[tokio::test]
    async fn logical_database_bound_refuses_before_remaining_source_is_polled() {
        let io = host(
            vec![(
                ROOT,
                vec![
                    seed("s3://bucket/warehouse/a.db/", true),
                    seed("s3://bucket/warehouse/b.db/", true),
                    seed("s3://bucket/warehouse/c.db/", true),
                ],
            )],
            &[],
        );
        let error = catalog(io.clone())
            .list_databases(ConnectorListingBound {
                entries: 1,
                ..ConnectorListingBound::V1
            })
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
        assert!(error.message().contains("entries bound"));
        assert_eq!(io.polls.load(Ordering::SeqCst), 2);
        assert_eq!(io.drops.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn physical_filtered_entries_consume_workspace_before_sdk_push() {
        let seeds = vec![
            seed("s3://bucket/warehouse/ignored", false),
            seed("s3://bucket/warehouse/other/", true),
            seed("s3://bucket/warehouse/a.db/", true),
        ];
        let io = host(vec![(ROOT, seeds.clone())], &[]);
        let bounded =
            BoundedListingIo::new(io.clone(), control(), ROOT, None, ConnectorListingBound::V1)
                .unwrap();
        {
            let mut budget = bounded.budget.lock().unwrap();
            budget.workspace_limit = budget.workspace_bytes + cost(&seeds[0]);
        }
        let error = sdk(Arc::new(bounded), control())
            .list_databases_plain()
            .await
            .unwrap_err();
        assert_eq!(
            map_sdk_error(error).kind(),
            ConnectorErrorKind::ResourceExhausted
        );
        assert_eq!(io.polls.load(Ordering::SeqCst), 2);
        assert_eq!(io.drops.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn spare_path_capacity_is_charged_not_just_its_length() {
        let mut entry = seed("s3://bucket/warehouse/a.db/", true);
        entry.capacity = 1024;
        let io = host(vec![(ROOT, vec![entry.clone(), entry.clone()])], &[]);
        let bounded =
            BoundedListingIo::new(io.clone(), control(), ROOT, None, ConnectorListingBound::V1)
                .unwrap();
        {
            let mut budget = bounded.budget.lock().unwrap();
            budget.workspace_limit = budget.workspace_bytes + cost(&entry) - 1;
        }
        let error = sdk(Arc::new(bounded), control())
            .list_databases_plain()
            .await
            .unwrap_err();
        assert_eq!(
            map_sdk_error(error).kind(),
            ConnectorErrorKind::ResourceExhausted
        );
        assert_eq!(io.polls.load(Ordering::SeqCst), 1);
        assert_eq!(io.drops.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn schema_fallback_shares_root_workspace_and_refuses_whole_listing() {
        let table = seed("s3://bucket/warehouse/db.db/t/", true);
        let schema = seed("s3://bucket/warehouse/db.db/t/schema/schema-7", false);
        let io = host(
            vec![
                ("s3://bucket/warehouse/db.db", vec![table.clone()]),
                (
                    "s3://bucket/warehouse/db.db/t/schema",
                    vec![schema.clone(), schema.clone()],
                ),
            ],
            &["s3://bucket/warehouse/db.db"],
        );
        let bounded = BoundedListingIo::new(
            io.clone(),
            control(),
            ROOT,
            Some("db"),
            ConnectorListingBound::V1,
        )
        .unwrap();
        {
            let mut budget = bounded.budget.lock().unwrap();
            budget.workspace_limit = budget.workspace_bytes + cost(&table) + cost(&schema) - 1;
        }
        let error = sdk(Arc::new(bounded), control())
            .list_tables_plain("db")
            .await
            .unwrap_err();
        assert_eq!(
            map_sdk_error(error).kind(),
            ConnectorErrorKind::ResourceExhausted
        );
        assert_eq!(io.calls.load(Ordering::SeqCst), 2);
        assert_eq!(io.polls.load(Ordering::SeqCst), 2);
        assert_eq!(io.drops.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn bounded_listing_matches_sdk_filters_and_schema_eligibility() {
        let io = host(
            vec![
                (
                    ROOT,
                    vec![
                        seed("s3://bucket/warehouse/db.db/", true),
                        seed("s3://bucket/warehouse/file.db", false),
                        seed("s3://bucket/warehouse/ignored/", true),
                    ],
                ),
                (
                    "s3://bucket/warehouse/db.db",
                    vec![
                        seed("s3://bucket/warehouse/db.db/fast/", true),
                        seed("s3://bucket/warehouse/db.db/later/", true),
                        seed("s3://bucket/warehouse/db.db/empty/", true),
                        seed("s3://bucket/warehouse/db.db/file", false),
                    ],
                ),
                (
                    "s3://bucket/warehouse/db.db/later/schema",
                    vec![
                        seed("s3://bucket/warehouse/db.db/later/schema/schema-7", false),
                        seed("s3://bucket/warehouse/db.db/later/schema/schema-2", false),
                        seed("s3://bucket/warehouse/db.db/later/schema/schema-bad", false),
                        seed("s3://bucket/warehouse/db.db/later/schema/schema-9/", true),
                    ],
                ),
            ],
            &[
                "s3://bucket/warehouse/db.db",
                "s3://bucket/warehouse/db.db/fast/schema/schema-0",
            ],
        );
        let plain = sdk(io.clone(), control());
        let catalog = catalog(io);
        assert_eq!(
            catalog
                .list_databases(ConnectorListingBound::V1)
                .await
                .unwrap()
                .entries(),
            plain.list_databases_plain().await.unwrap()
        );
        let actual = catalog
            .list_tables(
                "db",
                ConnectorListingBound {
                    entries: 2,
                    ..ConnectorListingBound::V1
                },
            )
            .await
            .unwrap();
        assert_eq!(
            actual.entries(),
            plain.list_tables_plain("db").await.unwrap()
        );
        assert_eq!(actual.entries(), &["fast", "later"]);
    }

    #[tokio::test]
    async fn liveness_refusal_drops_source_without_polling() {
        let io = host(
            vec![(ROOT, vec![seed("s3://bucket/warehouse/a.db/", true)])],
            &[],
        );
        let stop = Arc::new(novarocks_spi::connector::ConnectorStopOwner::new());
        let control =
            PaimonRequestControl::new(stop.view(), Instant::now() + Duration::from_secs(60));
        let bounded =
            BoundedListingIo::new(io.clone(), control, ROOT, None, ConnectorListingBound::V1)
                .unwrap();
        let mut stream = bounded.list(ROOT, false).await.unwrap();
        stop.request_stop();
        assert_eq!(
            map_sdk_error(stream.next().await.unwrap().unwrap_err()).kind(),
            ConnectorErrorKind::Cancelled
        );
        assert!(stream.next().await.is_none());
        assert_eq!(io.polls.load(Ordering::SeqCst), 0);
        assert_eq!(io.drops.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn deadline_refusal_keeps_its_kind_and_does_not_open_source() {
        let io = host(
            vec![(ROOT, vec![seed("s3://bucket/warehouse/a.db/", true)])],
            &[],
        );
        let control = PaimonRequestControl::new(
            Arc::new(novarocks_spi::connector::ConnectorStopOwner::new()).view(),
            Instant::now() - Duration::from_secs(1),
        );
        let bounded =
            BoundedListingIo::new(io.clone(), control, ROOT, None, ConnectorListingBound::V1)
                .unwrap();
        let error = match bounded.list(ROOT, false).await {
            Ok(_) => panic!("expired listing accepted"),
            Err(error) => error,
        };
        assert_eq!(
            map_sdk_error(error).kind(),
            ConnectorErrorKind::DeadlineExceeded
        );
        assert_eq!(io.calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn construction_copies_are_preflighted_and_bounds_cannot_be_relaxed() {
        let bound = ConnectorListingBound::V1;
        let warehouse = "x".repeat(WORKSPACE_BYTES / 8);
        assert_eq!(
            SourceBudget::new(&warehouse, None, bound, WORKSPACE_BYTES)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::ResourceExhausted
        );
        assert!(SourceBudget::new(ROOT, Some("db"), bound, WORKSPACE_BYTES + 1).is_err());
        assert!(
            SourceBudget::new(
                ROOT,
                None,
                ConnectorListingBound {
                    entries: bound.entries + 1,
                    ..bound
                },
                WORKSPACE_BYTES
            )
            .is_err()
        );
    }
}
