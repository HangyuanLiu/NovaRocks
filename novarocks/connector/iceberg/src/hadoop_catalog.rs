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

/// HadoopFileSystemCatalog — a Hadoop-catalog-compatible implementation of the
/// iceberg `Catalog` trait.
///
/// Differences from `MemoryCatalog`:
/// - Metadata files are written as `v{N}.metadata.json` (Hadoop convention).
/// - `version-hint.text` is maintained alongside each metadata directory so
///   that StarRocks FE, Spark, and Trino can discover the current version.
/// - `update_table` manually applies requirements/updates instead of delegating
///   to `TableCommit::apply()`, which calls `MetadataLocation::from_str()` and
///   only accepts the `{version}-{uuid}.metadata.json` format.
use std::collections::HashMap;
use std::sync::Arc;

use crate::iceberg::io::FileIO;
use crate::iceberg::spec::{TableMetadata, TableMetadataBuilder};
use crate::iceberg::table::Table;
use crate::iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent,
};
use async_trait::async_trait;
use bytes::Bytes;
use novarocks_fs::{ConditionalCreateOutcome, FileCancellation, FileErrorKind, ObjectStoreConfig};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HadoopCreateAttemptFacts {
    pub(crate) operation_id: String,
    pub(crate) table_uuid: String,
    pub(crate) metadata_location: String,
    pub(crate) metadata_digest: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HadoopCreateDisposition {
    Created,
    Existing,
}

#[derive(Debug)]
pub(crate) struct HadoopCreateResult {
    pub(crate) disposition: HadoopCreateDisposition,
    pub(crate) facts: HadoopCreateAttemptFacts,
    pub(crate) authoritative_table_uuid: String,
    pub(crate) authoritative_metadata_digest: String,
    pub(crate) table: Table,
    pub(crate) finalization_failure: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HadoopCreateFailureKind {
    Invalid,
    Unsupported,
    Unknown,
}

#[derive(Debug)]
pub(crate) struct HadoopCreateFailure {
    pub(crate) kind: HadoopCreateFailureKind,
    pub(crate) facts: Option<HadoopCreateAttemptFacts>,
    pub(crate) message: String,
}

#[derive(Debug)]
pub(crate) struct HadoopCreateAttempt {
    ident: TableIdent,
    facts: HadoopCreateAttemptFacts,
    table_location: String,
    metadata: TableMetadata,
    metadata_bytes: Bytes,
}

impl HadoopCreateAttempt {
    pub(crate) fn facts(&self) -> &HadoopCreateAttemptFacts {
        &self.facts
    }
}

#[derive(Debug)]
pub(crate) enum HadoopCreateReconciliation {
    Committed {
        finalization_failure: Option<String>,
    },
    Absent,
    Foreign,
}

#[derive(Debug)]
pub struct HadoopFileSystemCatalog {
    file_io: FileIO,
    warehouse_location: String,
    object_store_config: Option<ObjectStoreConfig>,
    /// Maps `"namespace/table"` to the current metadata file location.
    tables: Mutex<HashMap<String, String>>,
}

impl HadoopFileSystemCatalog {
    /// Create a new catalog backed by `file_io` writing under `warehouse_location`.
    pub fn new(file_io: FileIO, warehouse_location: String) -> Self {
        Self::new_with_object_store_config(file_io, warehouse_location, None)
    }

    pub(crate) fn new_with_object_store_config(
        file_io: FileIO,
        warehouse_location: String,
        object_store_config: Option<ObjectStoreConfig>,
    ) -> Self {
        Self {
            file_io,
            warehouse_location: warehouse_location.trim_end_matches('/').to_string(),
            object_store_config,
            tables: Mutex::new(HashMap::new()),
        }
    }

    // -----------------------------------------------------------------------
    // Path helpers (pub(crate) for unit tests)
    // -----------------------------------------------------------------------

    /// Returns the table root location derived from the warehouse location and
    /// the table identifier, e.g. `oss://bucket/warehouse/ns/table`.
    pub fn table_location(&self, ident: &TableIdent) -> String {
        let namespace = ident.namespace().join("/");
        format!("{}/{}/{}", self.warehouse_location, namespace, ident.name())
    }

    fn namespace_marker_location(&self, namespace: &NamespaceIdent) -> String {
        format!(
            "{}/{}/.novarocks_namespace",
            self.warehouse_location,
            namespace.join("/")
        )
    }

    fn namespace_location(&self, namespace: &NamespaceIdent) -> String {
        format!("{}/{}", self.warehouse_location, namespace.join("/"))
    }

    async fn external_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        let namespace_location = self.namespace_location(namespace);
        let mut tables = Vec::new();
        for table in self.file_io.list_directories(&namespace_location).await? {
            if self
                .file_io
                .exists(Self::version_hint_path(&format!(
                    "{namespace_location}/{table}"
                )))
                .await?
            {
                tables.push(TableIdent::new(namespace.clone(), table));
            }
        }
        tables.sort_by(|left, right| left.name().cmp(right.name()));
        tables.dedup();
        Ok(tables)
    }

    /// Returns the path to the `vN.metadata.json` file for a given table location
    /// and version number.
    pub fn metadata_path(table_location: &str, version: u32) -> String {
        let base = table_location.trim_end_matches('/');
        format!("{}/metadata/v{}.metadata.json", base, version)
    }

    /// Returns the path to the `version-hint.text` file for a given table location.
    pub fn version_hint_path(table_location: &str) -> String {
        let base = table_location.trim_end_matches('/');
        format!("{}/metadata/version-hint.text", base)
    }

    /// Read the current version stored in `version-hint.text`. Returns `0` if
    /// the file does not exist or cannot be parsed.
    async fn read_version_hint(&self, table_location: &str) -> u32 {
        let path = Self::version_hint_path(table_location);
        let Ok(input) = self.file_io.new_input(&path) else {
            return 0;
        };
        let Ok(bytes) = input.read().await else {
            return 0;
        };
        let s = String::from_utf8_lossy(&bytes);
        s.trim().parse::<u32>().unwrap_or(0)
    }

    /// Write `version-hint.text` with the given version number.
    async fn write_version_hint(&self, table_location: &str, version: u32) -> Result<()> {
        let path = Self::version_hint_path(table_location);
        let output = self.file_io.new_output(&path)?;
        output.write(format!("{}\n", version).into()).await
    }

    /// Persist table metadata at `v{version}.metadata.json` and update
    /// `version-hint.text`.
    async fn write_metadata(
        &self,
        table_location: &str,
        metadata: &TableMetadata,
        version: u32,
    ) -> Result<String> {
        let metadata_path = Self::metadata_path(table_location, version);
        metadata
            .write_to(&self.file_io, &metadata_path)
            .await
            .map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("write metadata to {}: {}", metadata_path, e),
                )
            })?;
        self.write_version_hint(table_location, version).await?;
        Ok(metadata_path)
    }

    /// Build a `Table` value from metadata and a metadata location.
    fn build_table(
        &self,
        ident: TableIdent,
        metadata: TableMetadata,
        metadata_location: String,
    ) -> Result<Table> {
        Table::builder()
            .file_io(self.file_io.clone())
            .metadata(Arc::new(metadata))
            .identifier(ident)
            .metadata_location(metadata_location)
            .build()
    }

    /// Return the table key used as the key in the `tables` map.
    fn table_key(ident: &TableIdent) -> String {
        let namespace = ident.namespace().join("/");
        format!("{}/{}", namespace, ident.name())
    }

    /// Read the current version hint from the filesystem and, if valid, insert
    /// the resolved metadata location into the in-memory cache.
    ///
    /// Returns `Some(metadata_location)` when the table exists on disk, or
    /// `None` when `version-hint.text` is absent or unparseable.  Both
    /// `load_table` and `table_exists` delegate to this helper so that every
    /// filesystem probe also populates the cache, making subsequent calls
    /// cache-hit fast.
    async fn try_cache_existing_table(&self, table: &TableIdent) -> Result<Option<String>> {
        let table_location = self.table_location(table);
        let version = self.read_version_hint(&table_location).await;
        let metadata_location = if version == 0 {
            let v1 = Self::metadata_path(&table_location, 1);
            if !self.file_io.exists(&v1).await? {
                return Ok(None);
            }
            TableMetadata::read_from(&self.file_io, &v1)
                .await
                .map_err(|error| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("read canonical Hadoop table metadata {v1}: {error}"),
                    )
                })?;
            if let Err(error) = self.write_version_hint(&table_location, 1).await {
                tracing::warn!(
                    "failed to repair Hadoop catalog version hint from canonical v1 metadata: {error}"
                );
            }
            v1
        } else {
            let hinted = Self::metadata_path(&table_location, version);
            if !self.file_io.exists(&hinted).await? {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("Hadoop catalog version hint points to missing metadata: {hinted}"),
                ));
            }
            TableMetadata::read_from(&self.file_io, &hinted)
                .await
                .map_err(|error| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("read hinted Hadoop table metadata {hinted}: {error}"),
                    )
                })?;
            hinted
        };
        let key = Self::table_key(table);
        self.tables
            .lock()
            .await
            .insert(key, metadata_location.clone());
        Ok(Some(metadata_location))
    }

    pub(crate) fn prepare_create_attempt(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
        operation_id: String,
    ) -> std::result::Result<HadoopCreateAttempt, HadoopCreateFailure> {
        let ident = TableIdent::new(namespace.clone(), creation.name.clone());
        let table_location = creation
            .location
            .clone()
            .unwrap_or_else(|| self.table_location(&ident));
        let creation_with_location = TableCreation {
            location: Some(table_location.clone()),
            ..creation
        };
        let build_result = TableMetadataBuilder::from_table_creation(creation_with_location)
            .map_err(|error| HadoopCreateFailure {
                kind: HadoopCreateFailureKind::Invalid,
                facts: None,
                message: format!("build metadata from creation: {error}"),
            })?
            .build()
            .map_err(|error| HadoopCreateFailure {
                kind: HadoopCreateFailureKind::Invalid,
                facts: None,
                message: format!("build metadata: {error}"),
            })?;
        let metadata = build_result.metadata;
        let metadata_bytes =
            serde_json::to_vec(&metadata).map_err(|error| HadoopCreateFailure {
                kind: HadoopCreateFailureKind::Invalid,
                facts: None,
                message: format!("serialize Hadoop table metadata: {error}"),
            })?;
        let metadata_location = Self::metadata_path(&table_location, 1);
        let facts = HadoopCreateAttemptFacts {
            operation_id,
            table_uuid: metadata.uuid().to_string(),
            metadata_location,
            metadata_digest: hex_digest(&metadata_bytes),
        };
        Ok(HadoopCreateAttempt {
            ident,
            facts,
            table_location,
            metadata,
            metadata_bytes: metadata_bytes.into(),
        })
    }

    pub(crate) async fn create_table_fenced(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
        operation_id: String,
    ) -> std::result::Result<HadoopCreateResult, HadoopCreateFailure> {
        let attempt = self.prepare_create_attempt(namespace, creation, operation_id)?;
        self.publish_create_attempt(attempt).await
    }

    pub(crate) async fn publish_create_attempt(
        &self,
        attempt: HadoopCreateAttempt,
    ) -> std::result::Result<HadoopCreateResult, HadoopCreateFailure> {
        let access = crate::fs_io::resolve_access_for_location(
            &attempt.facts.metadata_location,
            self.object_store_config.as_ref(),
        )
        .map_err(|message| HadoopCreateFailure {
            kind: HadoopCreateFailureKind::Invalid,
            facts: Some(attempt.facts.clone()),
            message: format!("resolve Hadoop metadata fence: {message}"),
        })?;

        // Capability validation precedes directory creation so an unsupported
        // binding fails before any metadata-side storage mutation.
        if !access.supports_conditional_create() {
            return Err(HadoopCreateFailure {
                kind: HadoopCreateFailureKind::Unsupported,
                facts: Some(attempt.facts.clone()),
                message: "Hadoop catalog storage does not support native conditional create"
                    .to_string(),
            });
        }
        access
            .ensure_parent_directory()
            .await
            .map_err(|message| HadoopCreateFailure {
                kind: HadoopCreateFailureKind::Unknown,
                facts: Some(attempt.facts.clone()),
                message: format!("create Hadoop metadata directory: {message}"),
            })?;

        let cancellation = FileCancellation::new();
        let conditional = access
            .handle()
            .create_if_absent(0, attempt.metadata_bytes.clone(), &cancellation)
            .await;
        let (disposition, metadata, authoritative_metadata_digest) = match conditional {
            Ok(ConditionalCreateOutcome::Created) => (
                HadoopCreateDisposition::Created,
                attempt.metadata.clone(),
                attempt.facts.metadata_digest.clone(),
            ),
            Ok(ConditionalCreateOutcome::AlreadyExists) => {
                self.classify_existing_v1(&attempt).await?
            }
            Err(error) if error.kind() == FileErrorKind::Unsupported => {
                return Err(HadoopCreateFailure {
                    kind: HadoopCreateFailureKind::Unsupported,
                    facts: Some(attempt.facts.clone()),
                    message: error.to_string(),
                });
            }
            Err(error) => match self.classify_existing_v1(&attempt).await {
                Ok(classified) => classified,
                Err(_) => {
                    return Err(HadoopCreateFailure {
                        kind: HadoopCreateFailureKind::Unknown,
                        facts: Some(attempt.facts.clone()),
                        message: format!(
                            "conditionally create Hadoop v1 metadata and authoritative reread failed: {error}"
                        ),
                    });
                }
            },
        };

        let mut finalization_failure = None;
        if disposition == HadoopCreateDisposition::Created {
            if let Err(error) = self.write_version_hint(&attempt.table_location, 1).await {
                finalization_failure = Some(format!(
                    "publish Hadoop catalog version hint after committed v1 metadata: {error}"
                ));
            }
        }

        let key = Self::table_key(&attempt.ident);
        self.tables
            .lock()
            .await
            .insert(key, attempt.facts.metadata_location.clone());
        let table = self
            .build_table(
                attempt.ident,
                metadata,
                attempt.facts.metadata_location.clone(),
            )
            .map_err(|error| HadoopCreateFailure {
                kind: HadoopCreateFailureKind::Unknown,
                facts: Some(attempt.facts.clone()),
                message: format!("build committed Hadoop table: {error}"),
            })?;
        Ok(HadoopCreateResult {
            disposition,
            facts: attempt.facts,
            authoritative_table_uuid: table.metadata().uuid().to_string(),
            authoritative_metadata_digest,
            table,
            finalization_failure,
        })
    }

    async fn classify_existing_v1(
        &self,
        attempt: &HadoopCreateAttempt,
    ) -> std::result::Result<(HadoopCreateDisposition, TableMetadata, String), HadoopCreateFailure>
    {
        let input = self
            .file_io
            .new_input(&attempt.facts.metadata_location)
            .map_err(|error| HadoopCreateFailure {
                kind: HadoopCreateFailureKind::Unknown,
                facts: Some(attempt.facts.clone()),
                message: format!("open authoritative Hadoop v1 metadata: {error}"),
            })?;
        let bytes = input.read().await.map_err(|error| HadoopCreateFailure {
            kind: HadoopCreateFailureKind::Unknown,
            facts: Some(attempt.facts.clone()),
            message: format!("read authoritative Hadoop v1 metadata: {error}"),
        })?;
        let metadata: TableMetadata =
            serde_json::from_slice(&bytes).map_err(|error| HadoopCreateFailure {
                kind: HadoopCreateFailureKind::Unknown,
                facts: Some(attempt.facts.clone()),
                message: format!("decode authoritative Hadoop v1 metadata: {error}"),
            })?;
        let authoritative_digest = hex_digest(&bytes);
        let same_owner = metadata.uuid().to_string() == attempt.facts.table_uuid
            && authoritative_digest == attempt.facts.metadata_digest;
        Ok((
            if same_owner {
                HadoopCreateDisposition::Created
            } else {
                HadoopCreateDisposition::Existing
            },
            metadata,
            authoritative_digest,
        ))
    }

    pub(crate) async fn reconcile_create_attempt(
        &self,
        namespace: &str,
        table: &str,
        expected_uuid: &str,
        expected_metadata_location: &str,
        expected_metadata_digest: &str,
    ) -> std::result::Result<HadoopCreateReconciliation, String> {
        let ident = TableIdent::from_strs([namespace, table])
            .map_err(|error| format!("build Hadoop table identity: {error}"))?;
        let table_location = self.table_location(&ident);
        let canonical_v1 = Self::metadata_path(&table_location, 1);
        if canonical_v1 != expected_metadata_location {
            return Ok(HadoopCreateReconciliation::Foreign);
        }
        if !self
            .file_io
            .exists(&canonical_v1)
            .await
            .map_err(|error| format!("probe authoritative Hadoop v1 metadata: {error}"))?
        {
            return Ok(HadoopCreateReconciliation::Absent);
        }
        let bytes = self
            .file_io
            .new_input(&canonical_v1)
            .map_err(|error| format!("open authoritative Hadoop v1 metadata: {error}"))?
            .read()
            .await
            .map_err(|error| format!("read authoritative Hadoop v1 metadata: {error}"))?;
        let metadata: TableMetadata = serde_json::from_slice(&bytes)
            .map_err(|error| format!("decode authoritative Hadoop v1 metadata: {error}"))?;
        if metadata.uuid().to_string() != expected_uuid
            || hex_digest(&bytes) != expected_metadata_digest
        {
            return Ok(HadoopCreateReconciliation::Foreign);
        }
        let finalization_failure = self
            .write_version_hint(&table_location, 1)
            .await
            .err()
            .map(|error| format!("repair committed Hadoop catalog version hint: {error}"));
        Ok(HadoopCreateReconciliation::Committed {
            finalization_failure,
        })
    }
}

fn hex_digest(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[async_trait]
impl Catalog for HadoopFileSystemCatalog {
    async fn list_namespaces(
        &self,
        _parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        Ok(vec![])
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        let marker = self.namespace_marker_location(namespace);
        self.file_io
            .new_output(&marker)?
            .write(Vec::new().into())
            .await?;
        Ok(Namespace::with_properties(namespace.clone(), properties))
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        Ok(Namespace::new(namespace.clone()))
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        if self
            .file_io
            .exists(self.namespace_marker_location(namespace))
            .await?
        {
            return Ok(true);
        }
        // Hadoop catalogs do not have a standard namespace metadata object.
        // External engines such as Spark establish one through direct table
        // directories. Inspect only direct children and their version hints;
        // never scan data files below the namespace.
        Ok(!self.external_tables(namespace).await?.is_empty())
    }

    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<()> {
        Ok(())
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        self.file_io
            .delete(self.namespace_marker_location(namespace))
            .await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        self.external_tables(namespace).await
    }

    /// Create a table: write `v1.metadata.json` and `version-hint.text=1`.
    ///
    /// If `creation.location` is `None` the table location is inferred from
    /// the warehouse location and the table identifier.
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        let result = self
            .create_table_fenced(namespace, creation, uuid::Uuid::now_v7().to_string())
            .await
            .map_err(|failure| {
                let kind = match failure.kind {
                    HadoopCreateFailureKind::Invalid => ErrorKind::DataInvalid,
                    HadoopCreateFailureKind::Unsupported => ErrorKind::FeatureUnsupported,
                    HadoopCreateFailureKind::Unknown => ErrorKind::Unexpected,
                };
                Error::new(kind, failure.message)
            })?;
        if result.disposition == HadoopCreateDisposition::Existing {
            return Err(Error::new(
                ErrorKind::TableAlreadyExists,
                format!("table already exists: {}", result.table.identifier()),
            ));
        }
        Ok(result.table)
    }

    /// Load a table from its registered metadata location.
    ///
    /// When the table is not in the in-memory registry (e.g. after a server
    /// restart or when a different catalog instance created the table), this
    /// falls back to reading `version-hint.text` from the filesystem to locate
    /// the current metadata file.
    async fn load_table(&self, table: &TableIdent) -> Result<Table> {
        let key = Self::table_key(table);
        let metadata_location = {
            let cached = self.tables.lock().await.get(&key).cloned();
            if let Some(loc) = cached {
                loc
            } else {
                // Fall back to filesystem: read version-hint.text written by
                // create_table and populate the cache via try_cache_existing_table.
                self.try_cache_existing_table(table).await?.ok_or_else(|| {
                    Error::new(
                        ErrorKind::TableNotFound,
                        format!("table not found: {}", key),
                    )
                })?
            }
        };

        let metadata = TableMetadata::read_from(&self.file_io, &metadata_location)
            .await
            .map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("read metadata from {}: {}", metadata_location, e),
                )
            })?;

        self.build_table(table.clone(), metadata, metadata_location)
    }

    async fn drop_table(&self, table: &TableIdent) -> Result<()> {
        let key = Self::table_key(table);
        self.tables.lock().await.remove(&key);

        // Physically delete the table's warehouse directory (metadata/ and data/
        // sub-directories) so that files are not left orphaned after DROP.
        // The table location follows the Hadoop catalog convention:
        //   <warehouse>/<namespace>/<table>
        let table_location = self.table_location(table);
        if let Err(e) = self.file_io.delete_prefix(&table_location).await {
            // Log but do not propagate — the in-memory and SQLite state has
            // already been removed, so the drop must be considered successful
            // even if the filesystem cleanup fails (e.g. table files were never
            // written because creation failed mid-way).
            tracing::warn!(
                "drop_table: failed to delete warehouse files for {table_location}: {e}"
            );
        }
        Ok(())
    }

    async fn table_exists(&self, table: &TableIdent) -> Result<bool> {
        let key = Self::table_key(table);
        if self.tables.lock().await.contains_key(&key) {
            return Ok(true);
        }
        // Fall back to filesystem: check version-hint.text and populate the
        // in-memory cache so that a subsequent load_table call is cache-hot.
        Ok(self.try_cache_existing_table(table).await?.is_some())
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        let src_key = Self::table_key(src);
        let dest_key = Self::table_key(dest);
        let mut guard = self.tables.lock().await;
        if let Some(loc) = guard.remove(&src_key) {
            guard.insert(dest_key, loc);
        }
        Ok(())
    }

    /// Register an existing table that already has metadata written at
    /// `metadata_location`.
    async fn register_table(&self, table: &TableIdent, metadata_location: String) -> Result<Table> {
        let metadata = TableMetadata::read_from(&self.file_io, &metadata_location)
            .await
            .map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("read metadata from {}: {}", metadata_location, e),
                )
            })?;

        let key = Self::table_key(table);
        self.tables
            .lock()
            .await
            .insert(key, metadata_location.clone());

        self.build_table(table.clone(), metadata, metadata_location)
    }

    /// Apply a table commit (requirements + updates) and write a new versioned
    /// metadata file.
    ///
    /// This method bypasses `TableCommit::apply()` which internally calls
    /// `MetadataLocation::from_str()`. That function rejects the Hadoop
    /// `vN.metadata.json` naming convention, so we manually apply requirements
    /// and updates here.
    async fn update_table(&self, mut commit: TableCommit) -> Result<Table> {
        let ident = commit.identifier().clone();

        // Load the current metadata.
        let current_table = self.load_table(&ident).await?;
        let current_metadata_location = current_table
            .metadata_location()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "no metadata location for table: {}",
                        Self::table_key(&ident)
                    ),
                )
            })?
            .to_string();
        let current_metadata = current_table.metadata();

        // Check all requirements against the current metadata.
        for requirement in commit.take_requirements() {
            requirement.check(Some(current_metadata))?;
        }

        // Apply all updates to produce new metadata.
        let mut builder = current_metadata
            .clone()
            .into_builder(Some(current_metadata_location));
        for update in commit.take_updates() {
            builder = update.apply(builder)?;
        }
        let new_metadata = builder.build()?.metadata;

        // Determine the next version number.
        let table_location = current_metadata.location().to_string();
        let current_version = self.read_version_hint(&table_location).await;
        let next_version = current_version + 1;

        // Write the new metadata and update version-hint.text.
        let new_metadata_location = self
            .write_metadata(&table_location, &new_metadata, next_version)
            .await?;

        // Update the in-memory registry.
        let key = Self::table_key(&ident);
        self.tables
            .lock()
            .await
            .insert(key, new_metadata_location.clone());

        self.build_table(ident, new_metadata, new_metadata_location)
    }
}

#[cfg(test)]
mod tests {
    use crate::iceberg::spec::{FormatVersion, NestedField, PrimitiveType, Schema, Type};

    use super::*;

    fn test_creation(name: &str) -> TableCreation {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("schema");
        TableCreation::builder()
            .name(name.to_string())
            .schema(schema)
            .format_version(FormatVersion::V2)
            .build()
    }

    #[test]
    fn test_metadata_path() {
        assert_eq!(
            HadoopFileSystemCatalog::metadata_path("oss://bucket/warehouse/db/tbl", 1),
            "oss://bucket/warehouse/db/tbl/metadata/v1.metadata.json"
        );
        assert_eq!(
            HadoopFileSystemCatalog::metadata_path("file:///tmp/wh/db/tbl", 3),
            "file:///tmp/wh/db/tbl/metadata/v3.metadata.json"
        );
    }

    #[test]
    fn test_version_hint_path() {
        assert_eq!(
            HadoopFileSystemCatalog::version_hint_path("oss://bucket/warehouse/db/tbl"),
            "oss://bucket/warehouse/db/tbl/metadata/version-hint.text"
        );
    }

    #[test]
    fn test_table_location() {
        let file_io = crate::fs_io::build_file_io_for_location("oss://bucket/warehouse", None);
        let catalog = HadoopFileSystemCatalog::new(file_io, "oss://bucket/warehouse".to_string());
        let ident = TableIdent::from_strs(["ns1", "my_table"]).unwrap();
        assert_eq!(
            catalog.table_location(&ident),
            "oss://bucket/warehouse/ns1/my_table"
        );
    }

    #[tokio::test]
    async fn namespace_marker_survives_catalog_reconstruction() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let location = warehouse.path().to_string_lossy().to_string();
        let namespace = NamespaceIdent::new("analytics".to_string());

        let catalog = HadoopFileSystemCatalog::new(
            crate::fs_io::build_file_io_for_location(&location, None),
            location.clone(),
        );
        assert!(!catalog.namespace_exists(&namespace).await.unwrap());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();
        assert!(catalog.namespace_exists(&namespace).await.unwrap());

        let restored = HadoopFileSystemCatalog::new(
            crate::fs_io::build_file_io_for_location(&location, None),
            location,
        );
        assert!(restored.namespace_exists(&namespace).await.unwrap());
        restored.drop_namespace(&namespace).await.unwrap();
        assert!(!restored.namespace_exists(&namespace).await.unwrap());
    }

    #[tokio::test]
    async fn external_table_directory_establishes_namespace_without_private_marker() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let location = warehouse.path().to_string_lossy().to_string();
        let namespace = NamespaceIdent::new("spark_created".to_string());
        let metadata_dir = warehouse
            .path()
            .join("spark_created")
            .join("orders")
            .join("metadata");
        std::fs::create_dir_all(&metadata_dir).expect("create Spark-style metadata directory");
        std::fs::write(metadata_dir.join("version-hint.text"), b"1\n")
            .expect("write Spark-style version hint");

        let catalog = HadoopFileSystemCatalog::new(
            crate::fs_io::build_file_io_for_location(&location, None),
            location,
        );
        assert!(catalog.namespace_exists(&namespace).await.unwrap());
        assert_eq!(
            catalog.list_tables(&namespace).await.unwrap(),
            vec![TableIdent::new(namespace, "orders".to_string())]
        );
    }

    #[tokio::test]
    async fn independent_catalog_clients_share_one_v1_owner() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let location = warehouse.path().to_string_lossy().to_string();
        let namespace = NamespaceIdent::new("analytics".to_string());
        let first = HadoopFileSystemCatalog::new(
            crate::fs_io::build_file_io_for_location(&location, None),
            location.clone(),
        );
        let second = HadoopFileSystemCatalog::new(
            crate::fs_io::build_file_io_for_location(&location, None),
            location,
        );

        let (left, right) = tokio::join!(
            first.create_table_fenced(
                &namespace,
                test_creation("events"),
                "operation-left".to_string(),
            ),
            second.create_table_fenced(
                &namespace,
                test_creation("events"),
                "operation-right".to_string(),
            ),
        );
        let left = left.expect("left create result");
        let right = right.expect("right create result");

        assert_eq!(
            [left.disposition, right.disposition]
                .into_iter()
                .filter(|disposition| *disposition == HadoopCreateDisposition::Created)
                .count(),
            1
        );
        assert_eq!(
            [left.disposition, right.disposition]
                .into_iter()
                .filter(|disposition| *disposition == HadoopCreateDisposition::Existing)
                .count(),
            1
        );
        assert_eq!(left.table.metadata().uuid(), right.table.metadata().uuid());
        assert_eq!(
            left.table.metadata_location(),
            right.table.metadata_location()
        );
    }

    #[tokio::test]
    async fn canonical_v1_recovers_table_when_hint_is_missing() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let location = warehouse.path().to_string_lossy().to_string();
        let namespace = NamespaceIdent::new("analytics".to_string());
        let ident = TableIdent::new(namespace.clone(), "events".to_string());
        let catalog = HadoopFileSystemCatalog::new(
            crate::fs_io::build_file_io_for_location(&location, None),
            location.clone(),
        );
        let created = catalog
            .create_table_fenced(
                &namespace,
                test_creation("events"),
                "operation-create".to_string(),
            )
            .await
            .expect("create table");
        let hint = HadoopFileSystemCatalog::version_hint_path(&catalog.table_location(&ident));
        catalog.file_io.delete(&hint).await.expect("remove hint");

        let restored = HadoopFileSystemCatalog::new(
            crate::fs_io::build_file_io_for_location(&location, None),
            location,
        );
        assert!(restored.table_exists(&ident).await.expect("table exists"));
        let loaded = restored.load_table(&ident).await.expect("load from v1");
        assert_eq!(created.table.metadata().uuid(), loaded.metadata().uuid());
        assert!(restored.file_io.exists(&hint).await.expect("repaired hint"));
    }
}
