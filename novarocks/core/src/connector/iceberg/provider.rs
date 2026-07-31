// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the License
// at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Iceberg's provider-owned implementation of the connector metadata/read SPI.
//!
//! The JSON payloads below are deliberately private to this provider.  They
//! contain only catalog/table identity and a snapshot pin; core code transports
//! them as opaque bytes and never downcasts into Iceberg objects.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::Instant;

use arrow::datatypes::{Schema, SchemaRef};
use bytes::Bytes;
use novarocks_catalog::schema::SqlType;
use novarocks_fs::{
    FileIdentity, FileIoRuntime, FileReadContext, FileTaskSpawner, FsAccessHandle,
    FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner,
};
use novarocks_spi::connector::{
    ConnectorBatchReader, ConnectorBeginScanRequest, ConnectorControlBinding, ConnectorError,
    ConnectorErrorKind, ConnectorExecutionBinding, ConnectorExecutionBindingKey,
    ConnectorExecutionDeclaration, ConnectorExecutionDistribution, ConnectorExecutionInstaller,
    ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorInstanceIncarnation,
    ConnectorListTablesRequest, ConnectorMetadata, ConnectorNamespaceRequest,
    ConnectorOpenReaderRequest, ConnectorProviderId, ConnectorReadExecution, ConnectorReadSelector,
    ConnectorScan, ConnectorScanHandle, ConnectorScanPlanning, ConnectorSplit,
    ConnectorSplitPlanningRequest, ConnectorTableHandle, ConnectorTableMetadata,
    ConnectorTableRequest, ConnectorTableResolution,
};
use serde::{Deserialize, Serialize};

use super::catalog::IcebergCatalogEntry;
use super::catalog::registry::{
    IcebergCatalogRegistry, extract_data_files_with_stats_at, list_tables, load_table,
};
use super::reader::IcebergBatchReader;
use super::scan_model::IcebergDataFileInfo;

#[derive(Clone, Deserialize, Serialize)]
struct IcebergDeltaSplitPayload {
    source: super::delta::DeltaSourceFile,
    #[serde(default)]
    delete_side: Option<super::delta::DeltaScanDeleteSide>,
}

const PROVIDER_ID: &str = "iceberg";
const MAX_CACHED_SNAPSHOT_MEMBERSHIPS: usize = 64;
const ICEBERG_DECLARATION_V1: u16 = 1;
const DEFAULT_ACCESS_BINDING: &str = "default";
/// Compat has no NovaRocks catalog instance identity on the wire.  Its
/// read-only Iceberg binding is therefore composed once per BE process, not
/// synthesized for each query or inferred from an HDFS path.
pub const COMPAT_ICEBERG_INSTANCE_ID: &str = "iceberg.compat.default";
const COMPAT_ICEBERG_INCARNATION: [u8; 16] = [0; 16];

/// Provider-owned, secret-free declaration used to install an Iceberg read
/// instance into a BE.  Catalog clients and credentials deliberately do not
/// cross this boundary: the installer resolves the named binding from process
/// startup composition.
#[derive(Deserialize, Serialize)]
struct IcebergDeclarationV1 {
    version: u16,
    access_binding: String,
}

#[derive(Clone)]
struct IcebergInstanceDistribution {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
}

impl ConnectorExecutionDistribution for IcebergInstanceDistribution {
    fn declaration(
        &self,
        context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<ConnectorExecutionDeclaration, ConnectorError> {
        if context.cancellation().is_cancelled() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Cancelled,
                "connector request was cancelled",
            ));
        }
        if Instant::now() >= context.deadline() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::DeadlineExceeded,
                "connector request deadline elapsed",
            ));
        }
        ConnectorExecutionDeclaration::try_new(
            self.descriptor.clone(),
            self.incarnation,
            encode_payload(
                &IcebergDeclarationV1 {
                    version: ICEBERG_DECLARATION_V1,
                    access_binding: DEFAULT_ACCESS_BINDING.to_string(),
                },
                "Iceberg execution declaration",
                novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            )?,
        )
    }
}

/// Process-startup binding for a read-only Iceberg execution instance.
///
/// This type intentionally owns only a locally-composed access binding.  It
/// has no catalog registry or metadata capability, because BE execution must
/// consume the fully planned provider split rather than reconnecting a
/// catalog.
#[derive(Clone)]
pub(crate) struct IcebergReadBinding {
    access_binding: String,
    object_store_config: Option<novarocks_fs::ObjectStoreConfig>,
    access_resolver: FsAccessResolver,
    file_runtime: Arc<dyn FileIoRuntime>,
    file_task_spawner: Arc<dyn FileTaskSpawner>,
}

impl std::fmt::Debug for IcebergReadBinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IcebergReadBinding")
            .field("access_binding", &self.access_binding)
            .field(
                "object_store_config",
                &self.object_store_config.as_ref().map(|_| "<redacted>"),
            )
            .finish_non_exhaustive()
    }
}

impl IcebergReadBinding {
    pub(crate) fn default_binding(
        object_store_config: Option<novarocks_fs::ObjectStoreConfig>,
    ) -> Result<Self, ConnectorError> {
        let handle = tokio::runtime::Handle::try_current().unwrap_or_else(|_| {
            static FALLBACK_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
            FALLBACK_RUNTIME
                .get_or_init(|| {
                    tokio::runtime::Builder::new_multi_thread()
                        .enable_all()
                        .build()
                        .expect("Iceberg fallback Tokio runtime must initialize")
                })
                .handle()
                .clone()
        });
        Ok(Self::new(
            object_store_config,
            Arc::new(TokioFileIoRuntime::new(handle.clone())),
            Arc::new(TokioFileTaskSpawner::new(handle)),
        ))
    }

    pub(crate) fn new(
        object_store_config: Option<novarocks_fs::ObjectStoreConfig>,
        file_runtime: Arc<dyn FileIoRuntime>,
        file_task_spawner: Arc<dyn FileTaskSpawner>,
    ) -> Self {
        Self {
            access_binding: DEFAULT_ACCESS_BINDING.to_string(),
            object_store_config,
            access_resolver: FsAccessResolver::new(),
            file_runtime,
            file_task_spawner,
        }
    }

    pub(crate) fn resolve_access(&self, location: &str) -> Result<FsAccessHandle, ConnectorError> {
        self.access_resolver
            .resolve_location(location, self.object_store_config.as_ref())
            .map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string())
            })
    }

    pub(crate) fn file_read_context(
        &self,
        cancellation: novarocks_fs::FileCancellation,
        deadline: std::time::Instant,
    ) -> Result<FileReadContext, ConnectorError> {
        Ok(FileReadContext {
            cancellation,
            deadline: Some(deadline),
            runtime: Arc::clone(&self.file_runtime),
            task_spawner: Arc::clone(&self.file_task_spawner),
        })
    }

    pub(crate) fn file_size(
        &self,
        path: &str,
        access: &FsAccessHandle,
        context: &FileReadContext,
    ) -> Result<u64, ConnectorError> {
        let file = access
            .bind_location(path, FileIdentity::new(path, 0, None))
            .map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string())
            })?;
        let cancellation = context.cancellation.clone();
        context
            .runtime
            .block_on_u64(Box::pin(async move { file.stat(&cancellation).await }))
            .map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::Unavailable, error.to_string())
                    .with_retryable_before_progress()
            })
    }
}

/// Startup-composed installer for Iceberg read-only instances.  The payload
/// identifies the binding but cannot override it with cloud properties.
pub(crate) struct IcebergConnectorInstaller {
    provider_id: ConnectorProviderId,
    binding: IcebergReadBinding,
}

impl IcebergConnectorInstaller {
    pub(crate) fn new(binding: IcebergReadBinding) -> Self {
        Self {
            provider_id: ConnectorProviderId::parse(PROVIDER_ID)
                .expect("static Iceberg provider ID is valid"),
            binding,
        }
    }
}

/// Build the stable compat-only Iceberg reader instance from the same startup
/// access binding used by distributed instance installation.  The identity is
/// intentionally not query-local and carries no catalog client or credential
/// in a fragment payload.
/// Compose the compat Iceberg reader as a BE-only execution binding. Compat
/// has no catalog metadata capability and must never manufacture one.
pub(crate) fn compose_compat_execution_binding(
    binding: IcebergReadBinding,
) -> Result<ConnectorExecutionBinding, ConnectorError> {
    let instance_id = ConnectorInstanceId::parse(COMPAT_ICEBERG_INSTANCE_ID)?;
    let key = ConnectorExecutionBindingKey {
        instance_id,
        incarnation: ConnectorInstanceIncarnation::from_bytes(COMPAT_ICEBERG_INCARNATION),
    };
    ConnectorExecutionBinding::try_new(
        ConnectorProviderId::parse(PROVIDER_ID)?,
        key.clone(),
        Arc::new(IcebergReadOnlyConnectorInstance { key, binding }),
    )
}

/// Encode compat-normalized data-file facts as provider-owned opaque splits.
/// The caller cannot alter provider identity, incarnation, or payload layout.
pub fn build_compat_read_splits(
    files: impl IntoIterator<Item = IcebergDataFileInfo>,
) -> Result<Vec<ConnectorSplit>, ConnectorError> {
    let owner = ConnectorInstanceId::parse(COMPAT_ICEBERG_INSTANCE_ID)?;
    files
        .into_iter()
        .enumerate()
        .map(|(index, data_file)| {
            let estimated_bytes = u64::try_from(data_file.size).ok();
            let payload = SplitPayload {
                version: ICEBERG_SPLIT_V1,
                owner_instance_id: owner.as_str().to_string(),
                incarnation: COMPAT_ICEBERG_INCARNATION,
                namespace: "compat".to_string(),
                table: "iceberg".to_string(),
                snapshot_id: None,
                table_uuid: None,
                schema_id: None,
                data_file,
                projection: Vec::new(),
                limit: None,
                delta: None,
            };
            ConnectorSplit::try_new(
                owner.clone(),
                format!("compat-iceberg-{index}"),
                encode_payload(
                    &payload,
                    "compat Iceberg split",
                    novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
                )?,
                estimated_bytes,
            )
        })
        .collect()
}

/// Encode compat-normalized snapshot-delta facts as ordinary provider-owned
/// splits.  The stable compat instance owns the startup access binding; the
/// Thrift plan contributes no object-store configuration or runtime handle.
pub fn build_compat_delta_read_splits(
    sources: impl IntoIterator<Item = super::delta::DeltaSourceFile>,
    delete_side: Option<super::delta::DeltaScanDeleteSide>,
) -> Result<Vec<ConnectorSplit>, ConnectorError> {
    let owner = ConnectorInstanceId::parse(COMPAT_ICEBERG_INSTANCE_ID)?;
    sources
        .into_iter()
        .enumerate()
        .map(|(index, source)| {
            let estimated_bytes = u64::try_from(source.size).ok();
            let data_file = IcebergDataFileInfo {
                path: source.path.clone(),
                size: source.size,
                row_count: None,
                column_stats: None,
                partition_spec_id: source.partition_spec_id,
                partition_key: source.partition_key.clone(),
                first_row_id: source.first_row_id,
                data_sequence_number: source.data_sequence_number,
                ivm_change_op: None,
                included_positions: None,
                delete_files: Vec::new(),
                manifest_path: None,
                partition_values: Vec::new(),
            };
            let payload = SplitPayload {
                version: ICEBERG_SPLIT_V1,
                owner_instance_id: owner.as_str().to_string(),
                incarnation: COMPAT_ICEBERG_INCARNATION,
                namespace: "compat".to_string(),
                table: "iceberg-delta".to_string(),
                snapshot_id: None,
                table_uuid: None,
                schema_id: None,
                data_file,
                projection: Vec::new(),
                limit: None,
                delta: Some(IcebergDeltaSplitPayload {
                    source,
                    delete_side: delete_side.clone(),
                }),
            };
            ConnectorSplit::try_new(
                owner.clone(),
                format!("compat-iceberg-delta-{index}"),
                encode_payload(
                    &payload,
                    "compat Iceberg delta split",
                    novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
                )?,
                estimated_bytes,
            )
        })
        .collect()
}

impl ConnectorExecutionInstaller for IcebergConnectorInstaller {
    fn provider_id(&self) -> &ConnectorProviderId {
        &self.provider_id
    }

    fn install(
        &self,
        declaration: &ConnectorExecutionDeclaration,
        _context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<ConnectorExecutionBinding, ConnectorError> {
        if declaration.descriptor().provider_id != self.provider_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg installer received a declaration for another provider",
            ));
        }
        let payload: IcebergDeclarationV1 =
            decode_payload(declaration.payload(), "Iceberg execution declaration")?;
        if payload.version != ICEBERG_DECLARATION_V1 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                format!(
                    "unsupported Iceberg execution declaration version {}",
                    payload.version
                ),
            ));
        }
        if payload.access_binding != self.binding.access_binding {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg declaration access binding does not match BE startup binding",
            ));
        }
        let key = declaration.binding_key();
        ConnectorExecutionBinding::try_new(
            self.provider_id.clone(),
            key.clone(),
            Arc::new(IcebergReadOnlyConnectorInstance {
                key,
                binding: self.binding.clone(),
            }),
        )
    }
}

/// BE-only instance installed through the binding control plane.  Metadata and
/// planning are deliberately unsupported; once the provider reader is wired,
/// `open_reader` will consume the opaque, fully planned Iceberg split.
struct IcebergReadOnlyConnectorInstance {
    key: ConnectorExecutionBindingKey,
    binding: IcebergReadBinding,
}

impl IcebergReadOnlyConnectorInstance {
    fn open_reader(
        &self,
        split: &ConnectorSplit,
        request: ConnectorOpenReaderRequest,
    ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
        if request.context.cancellation().is_cancelled() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Cancelled,
                "connector request was cancelled",
            ));
        }
        if Instant::now() >= request.context.deadline() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::DeadlineExceeded,
                "connector request deadline elapsed",
            ));
        }
        ensure_owner(split.owner(), &self.key.instance_id)?;
        let payload: SplitPayload = decode_payload(split.payload(), "Iceberg split")?;
        if payload.version != ICEBERG_SPLIT_V1 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                format!("unsupported Iceberg split version {}", payload.version),
            ));
        }
        if payload.owner_instance_id != self.key.instance_id.as_str()
            || payload.incarnation != self.key.incarnation.to_bytes()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg split does not belong to this installed instance incarnation",
            ));
        }
        if let Some(delta) = payload.delta {
            return super::delta_reader::IcebergDeltaBatchReader::try_new(
                delta.source,
                delta.delete_side,
                self.binding.clone(),
                request,
            )
            .map(|reader| Box::new(reader) as Box<dyn ConnectorBatchReader>);
        }
        let file_context = self.binding.file_read_context(
            novarocks_fs::FileCancellation::new(),
            request.context.deadline(),
        )?;
        let access = self.binding.resolve_access(&payload.data_file.path)?;
        IcebergBatchReader::try_new(&payload.data_file, access, request, file_context)
            .map(|reader| Box::new(reader) as Box<dyn ConnectorBatchReader>)
    }
}

impl ConnectorReadExecution for IcebergReadOnlyConnectorInstance {
    fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.key
    }

    fn open_reader(
        &self,
        split: &ConnectorSplit,
        request: ConnectorOpenReaderRequest,
    ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
        self.open_reader(split, request)
    }
}

#[derive(Clone)]
pub(crate) struct IcebergControlProvider {
    instance_id: ConnectorInstanceId,
    incarnation: ConnectorInstanceIncarnation,
    registry: Arc<RwLock<IcebergCatalogRegistry>>,
    snapshot_memberships: Arc<SnapshotMembershipCache>,
}

impl IcebergControlProvider {
    /// Creates the FE-only control binding for a logical Iceberg catalog. The
    /// implementation remains in core until SPI-5, but its runtime capability
    /// aggregate no longer needs to cross into a BE process.
    pub(crate) fn new_control(
        instance_id: ConnectorInstanceId,
        registry: Arc<RwLock<IcebergCatalogRegistry>>,
    ) -> Result<ConnectorControlBinding, ConnectorError> {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse(PROVIDER_ID)?,
            instance_id: instance_id.clone(),
        };
        let incarnation = ConnectorInstanceIncarnation::new();
        let provider = Arc::new(Self {
            instance_id,
            incarnation,
            registry,
            snapshot_memberships: Arc::new(SnapshotMembershipCache::new(
                MAX_CACHED_SNAPSHOT_MEMBERSHIPS,
            )),
        });
        ConnectorControlBinding::try_new(
            descriptor.clone(),
            incarnation,
            provider.clone(),
            provider,
            Arc::new(IcebergInstanceDistribution {
                descriptor,
                incarnation,
            }),
        )
    }

    fn entry(&self, catalog: &str) -> Result<IcebergCatalogEntry, ConnectorError> {
        self.registry
            .read()
            .map_err(|error| internal(format!("iceberg catalog registry read lock: {error}")))?
            .get(catalog)
            .map_err(map_iceberg_error)
    }

    fn validate_context(
        &self,
        context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<(), ConnectorError> {
        if context.cancellation().is_cancelled() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Cancelled,
                "connector request was cancelled",
            ));
        }
        if Instant::now() >= context.deadline() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::DeadlineExceeded,
                "connector request deadline elapsed",
            ));
        }
        Ok(())
    }

    fn table_payload(&self, table: &ConnectorTableHandle) -> Result<TablePayload, ConnectorError> {
        ensure_owner(table.owner(), &self.instance_id)?;
        decode_payload(table.payload(), "table handle")
    }

    fn scan_payload(&self, scan: &ConnectorScanHandle) -> Result<ScanPayload, ConnectorError> {
        ensure_owner(scan.owner(), &self.instance_id)?;
        decode_payload(scan.payload(), "scan handle")
    }

    fn schema_for(
        &self,
        entry: &IcebergCatalogEntry,
        namespace: &str,
        table: &str,
        projection: &[usize],
    ) -> Result<SchemaRef, ConnectorError> {
        let loaded = load_table(entry, namespace, table).map_err(map_iceberg_error)?;
        let storage_schema =
            iceberg::arrow::schema_to_arrow_schema(loaded.table.metadata().current_schema())
                .map_err(|error| internal(format!("convert Iceberg schema to Arrow: {error}")))?;
        let indexes = if projection.is_empty() {
            (0..storage_schema.fields().len()).collect::<Vec<_>>()
        } else {
            projection.to_vec()
        };
        let fields = indexes
            .into_iter()
            .map(|index| {
                let storage_field = storage_schema.fields().get(index).ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        format!("Iceberg projection index {index} is outside the table schema"),
                    )
                })?;
                let logical_column = loaded
                    .columns
                    .iter()
                    .find(|column| column.name.eq_ignore_ascii_case(storage_field.name()))
                    .ok_or_else(|| {
                        ConnectorError::new(
                            ConnectorErrorKind::CorruptData,
                            format!(
                                "Iceberg table schema field {} is missing its logical column definition",
                                storage_field.name()
                            ),
                        )
                    })?;
                Ok(Arc::new(
                    storage_field
                        .as_ref()
                        .clone()
                        .with_data_type(logical_column.data_type.clone()),
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Arc::new(Schema::new(fields)))
    }

    fn select_snapshot(
        &self,
        entry: &IcebergCatalogEntry,
        table: &TablePayload,
        selector: ConnectorReadSelector,
    ) -> Result<(Option<i64>, String), ConnectorError> {
        let loaded =
            load_table(entry, &table.namespace, &table.table).map_err(map_iceberg_error)?;
        let metadata = loaded.table.metadata();
        let snapshot_id = match selector {
            ConnectorReadSelector::Current => Ok(metadata.current_snapshot_id()),
            ConnectorReadSelector::SnapshotId(snapshot_id) => metadata
                .snapshot_by_id(snapshot_id)
                .map(|_| Some(snapshot_id))
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::NotFound,
                        format!("Iceberg snapshot {snapshot_id} was not found"),
                    )
                }),
            ConnectorReadSelector::TimestampMicros(timestamp_micros) => metadata
                .snapshots()
                .filter(|snapshot| {
                    snapshot.timestamp_ms().saturating_mul(1_000) <= timestamp_micros
                })
                .max_by_key(|snapshot| snapshot.timestamp_ms())
                .map(|snapshot| Some(snapshot.snapshot_id()))
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::NotFound,
                        format!("no Iceberg snapshot exists at timestamp {timestamp_micros}"),
                    )
                }),
        }?;
        Ok((snapshot_id, metadata.uuid().to_string()))
    }

    fn snapshot_membership(
        &self,
        entry: &IcebergCatalogEntry,
        namespace: &str,
        table: &str,
        table_uuid: &str,
        snapshot_id: i64,
    ) -> Result<Arc<HashSet<SnapshotFileIdentity>>, ConnectorError> {
        let key = SnapshotMembershipKey {
            namespace: namespace.to_string(),
            table: table.to_string(),
            table_uuid: table_uuid.to_string(),
            snapshot_id,
        };
        self.snapshot_memberships.get_or_try_init(key, || {
            let loaded = load_table(entry, namespace, table).map_err(map_iceberg_error)?;
            if loaded.table.metadata().uuid().to_string() != table_uuid {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "Iceberg split belongs to a different table incarnation",
                ));
            }
            extract_data_files_with_stats_at(&loaded.table, snapshot_id)
                .map_err(map_iceberg_error)
                .map(|files| {
                    Arc::new(
                        files
                            .into_iter()
                            .map(|file| SnapshotFileIdentity {
                                path: file.path,
                                size: file.size,
                                row_count: file.record_count,
                            })
                            .collect(),
                    )
                })
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct SnapshotMembershipKey {
    namespace: String,
    table: String,
    table_uuid: String,
    snapshot_id: i64,
}

#[derive(Debug, Eq, Hash, PartialEq)]
struct SnapshotFileIdentity {
    path: String,
    size: i64,
    row_count: Option<i64>,
}

impl From<&IcebergDataFileInfo> for SnapshotFileIdentity {
    fn from(file: &IcebergDataFileInfo) -> Self {
        Self {
            path: file.path.clone(),
            size: file.size,
            row_count: file.row_count,
        }
    }
}

type SnapshotMembership = Result<Arc<HashSet<SnapshotFileIdentity>>, ConnectorError>;
type SnapshotMembershipCell = Arc<OnceLock<SnapshotMembership>>;

struct SnapshotMembershipCache {
    capacity: usize,
    state: Mutex<SnapshotMembershipCacheState>,
}

#[derive(Default)]
struct SnapshotMembershipCacheState {
    entries: HashMap<SnapshotMembershipKey, SnapshotMembershipCell>,
    order: VecDeque<SnapshotMembershipKey>,
}

impl SnapshotMembershipCache {
    fn new(capacity: usize) -> Self {
        assert!(
            capacity > 0,
            "snapshot membership cache capacity must be nonzero"
        );
        Self {
            capacity,
            state: Mutex::new(SnapshotMembershipCacheState::default()),
        }
    }

    fn cell(&self, key: &SnapshotMembershipKey) -> Result<SnapshotMembershipCell, ConnectorError> {
        let mut state = self
            .state
            .lock()
            .map_err(|error| internal(format!("snapshot membership cache lock: {error}")))?;
        if let Some(cell) = state.entries.get(key).cloned() {
            state.order.retain(|cached| cached != key);
            state.order.push_back(key.clone());
            return Ok(cell);
        }
        while state.entries.len() >= self.capacity {
            let Some(evicted) = state.order.pop_front() else {
                return Err(internal(
                    "snapshot membership cache lost its eviction order".to_string(),
                ));
            };
            state.entries.remove(&evicted);
        }
        let cell = Arc::new(OnceLock::new());
        state.entries.insert(key.clone(), Arc::clone(&cell));
        state.order.push_back(key.clone());
        Ok(cell)
    }

    fn get_or_try_init(
        &self,
        key: SnapshotMembershipKey,
        load: impl FnOnce() -> SnapshotMembership,
    ) -> SnapshotMembership {
        let cell = self.cell(&key)?;
        let result = cell.get_or_init(load).clone();
        if result.is_err()
            && let Ok(mut state) = self.state.lock()
            && state
                .entries
                .get(&key)
                .is_some_and(|cached| Arc::ptr_eq(cached, &cell))
        {
            state.entries.remove(&key);
            state.order.retain(|cached| cached != &key);
        }
        result
    }

    fn insert(
        &self,
        key: SnapshotMembershipKey,
        membership: Arc<HashSet<SnapshotFileIdentity>>,
    ) -> Result<(), ConnectorError> {
        let cell = self.cell(&key)?;
        let _ = cell.set(Ok(membership));
        Ok(())
    }
}

impl ConnectorMetadata for IcebergControlProvider {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }

    fn namespace_exists(&self, request: ConnectorNamespaceRequest) -> Result<bool, ConnectorError> {
        self.validate_context(&request.context)?;
        ensure_owner(&request.namespace.instance_id, &self.instance_id)?;
        let entry = self.entry(self.instance_id.as_str())?;
        super::catalog::namespace_exists(&entry, &request.namespace.namespace)
            .map_err(map_iceberg_error)
    }

    fn table_exists(&self, request: ConnectorTableRequest) -> Result<bool, ConnectorError> {
        self.validate_context(&request.context)?;
        ensure_owner(&request.table.instance_id, &self.instance_id)?;
        let entry = self.entry(self.instance_id.as_str())?;
        let tables = list_tables(&entry, &request.table.namespace).map_err(map_iceberg_error)?;
        Ok(tables
            .iter()
            .any(|table| table.eq_ignore_ascii_case(&request.table.table)))
    }

    fn list_tables(
        &self,
        request: ConnectorListTablesRequest,
    ) -> Result<Vec<novarocks_spi::connector::ConnectorTableIdentity>, ConnectorError> {
        self.validate_context(&request.context)?;
        ensure_owner(&request.namespace.instance_id, &self.instance_id)?;
        let entry = self.entry(self.instance_id.as_str())?;
        list_tables(&entry, &request.namespace.namespace)
            .map_err(map_iceberg_error)?
            .into_iter()
            .map(|table| {
                Ok(novarocks_spi::connector::ConnectorTableIdentity {
                    instance_id: self.instance_id.clone(),
                    namespace: request.namespace.namespace.clone(),
                    table: Arc::from(table),
                })
            })
            .collect()
    }

    fn load_table(
        &self,
        request: ConnectorTableRequest,
    ) -> Result<ConnectorTableMetadata, ConnectorError> {
        self.validate_context(&request.context)?;
        ensure_owner(&request.table.instance_id, &self.instance_id)?;
        let entry = self.entry(self.instance_id.as_str())?;
        let requested_table = request.table.table.to_string();
        let (table_name, metadata_table_type) =
            resolve_table_request(&requested_table, request.resolution)?;
        let loaded =
            load_table(&entry, &request.table.namespace, &table_name).map_err(map_iceberg_error)?;
        let schema = self.schema_for(&entry, &request.table.namespace, &table_name, &[])?;
        let version = Some(Bytes::copy_from_slice(
            &loaded.table.metadata().current_schema_id().to_le_bytes(),
        ));
        let table = build_table_payload(
            &entry,
            self.instance_id.as_str(),
            &request.table.namespace,
            &table_name,
            loaded,
            metadata_table_type,
        )?;
        Ok(ConnectorTableMetadata {
            identity: novarocks_spi::connector::ConnectorTableIdentity {
                instance_id: self.instance_id.clone(),
                namespace: request.table.namespace,
                table: Arc::from(table_name),
            },
            schema,
            version,
            table: ConnectorTableHandle::try_new(
                self.instance_id.clone(),
                encode_payload(
                    &table,
                    "table handle",
                    request.context.max_handle_payload_bytes(),
                )?,
            )?,
        })
    }
}

impl ConnectorScanPlanning for IcebergControlProvider {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }

    fn begin_scan(
        &self,
        table: &ConnectorTableHandle,
        request: ConnectorBeginScanRequest,
    ) -> Result<ConnectorScan, ConnectorError> {
        self.validate_context(&request.context)?;
        let table = self.table_payload(table)?;
        let entry = self.entry(self.instance_id.as_str())?;
        let output_schema =
            self.schema_for(&entry, &table.namespace, &table.table, &request.projection)?;
        let (snapshot_id, table_uuid) = if table.explicit_files.is_some() {
            (None, None)
        } else {
            let (snapshot_id, table_uuid) =
                self.select_snapshot(&entry, &table, request.selector)?;
            (snapshot_id, Some(table_uuid))
        };
        let payload = ScanPayload {
            table,
            snapshot_id,
            table_uuid,
            projection: request.projection,
            limit: request.limit,
        };
        Ok(ConnectorScan {
            handle: ConnectorScanHandle::try_new(
                self.instance_id.clone(),
                encode_payload(
                    &payload,
                    "scan handle",
                    request.context.max_handle_payload_bytes(),
                )?,
            )?,
            output_schema,
        })
    }

    fn plan_splits(
        &self,
        scan: &ConnectorScanHandle,
        request: ConnectorSplitPlanningRequest,
    ) -> Result<Vec<ConnectorSplit>, ConnectorError> {
        self.validate_context(&request.context)?;
        let scan = self.scan_payload(scan)?;
        let files = match (&scan.table.explicit_files, scan.snapshot_id) {
            (Some(files), _) => files.clone(),
            (None, None) => Vec::new(),
            (None, Some(snapshot_id)) => {
                let entry = self.entry(self.instance_id.as_str())?;
                let loaded = load_table(&entry, &scan.table.namespace, &scan.table.table)
                    .map_err(map_iceberg_error)?;
                let table_uuid = scan.table_uuid.as_deref().ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::CorruptData,
                        "Iceberg snapshot scan is missing its table incarnation",
                    )
                })?;
                if loaded.table.metadata().uuid().to_string() != table_uuid {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::CorruptData,
                        "Iceberg scan belongs to a different table incarnation",
                    ));
                }
                extract_data_files_with_stats_at(&loaded.table, snapshot_id)
                    .map_err(map_iceberg_error)?
                    .into_iter()
                    .map(super::catalog::backend::data_file_with_stats_to_iceberg_data_file_info)
                    .collect()
            }
        };
        if let Some(snapshot_id) = scan.snapshot_id {
            let table_uuid = scan.table_uuid.as_deref().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "Iceberg snapshot scan is missing its table incarnation",
                )
            })?;
            self.snapshot_memberships.insert(
                SnapshotMembershipKey {
                    namespace: scan.table.namespace.clone(),
                    table: scan.table.table.clone(),
                    table_uuid: table_uuid.to_string(),
                    snapshot_id,
                },
                Arc::new(
                    files
                        .iter()
                        .map(SnapshotFileIdentity::from)
                        .collect::<HashSet<_>>(),
                ),
            )?;
        }
        let mut remaining = scan.limit;
        let mut splits = Vec::new();
        let mut total_payload_bytes = 0usize;
        for (index, file) in files.into_iter().enumerate() {
            if let Some(remaining_rows) = remaining.as_mut() {
                if *remaining_rows == 0 {
                    break;
                }
                if let Some(row_count) = file.row_count.and_then(|count| u64::try_from(count).ok())
                {
                    *remaining_rows = remaining_rows.saturating_sub(row_count);
                }
            }
            let estimated_bytes = u64::try_from(file.size).ok();
            let payload = SplitPayload {
                version: ICEBERG_SPLIT_V1,
                owner_instance_id: self.instance_id.as_str().to_string(),
                incarnation: self.incarnation.to_bytes(),
                namespace: scan.table.namespace.clone(),
                table: scan.table.table.clone(),
                snapshot_id: scan.snapshot_id,
                table_uuid: scan.table_uuid.clone(),
                schema_id: scan.table.table_info.as_ref().map(|table| table.schema_id),
                data_file: file,
                projection: scan.projection.clone(),
                limit: scan.limit,
                delta: None,
            };
            push_split_with_budget(
                &mut splits,
                &mut total_payload_bytes,
                self.instance_id.clone(),
                format!(
                    "{}-{index}",
                    scan.snapshot_id
                        .map(|snapshot_id| snapshot_id.to_string())
                        .unwrap_or_else(|| "explicit".to_string())
                ),
                &payload,
                estimated_bytes,
                &request.context,
            )?;
        }
        Ok(splits)
    }
}

fn resolve_table_request(
    requested_table: &str,
    resolution: ConnectorTableResolution,
) -> Result<(String, Option<super::IcebergMetadataTableType>), ConnectorError> {
    let alias = requested_table
        .rsplit_once('$')
        .and_then(|(table, suffix)| {
            super::IcebergMetadataTableType::parse(suffix)
                .ok()
                .map(|metadata_type| (table.to_string(), metadata_type))
        });
    match (resolution, alias) {
        (ConnectorTableResolution::StrictBaseTable, Some(_)) => Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "strict Iceberg table resolution does not accept metadata aliases",
        )),
        (ConnectorTableResolution::StrictBaseTable, None) => {
            Ok((requested_table.to_string(), None))
        }
        (ConnectorTableResolution::ProviderReadAlias, Some((table, metadata_type))) => {
            Ok((table, Some(metadata_type)))
        }
        (ConnectorTableResolution::ProviderReadAlias, None) => Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "Iceberg provider read alias must use `<table>$<metadata-type>`",
        )),
    }
}

fn build_table_payload(
    entry: &IcebergCatalogEntry,
    catalog: &str,
    namespace: &str,
    table: &str,
    loaded: super::catalog::IcebergLoadedTable,
    metadata_table_type: Option<super::IcebergMetadataTableType>,
) -> Result<TablePayload, ConnectorError> {
    let logical_type_columns = loaded
        .logical_types
        .iter()
        .filter_map(|(column_name, logical_type)| match logical_type {
            SqlType::Bitmap => Some((column_name.to_lowercase(), "bitmap".to_string())),
            SqlType::Hll => Some((column_name.to_lowercase(), "hll".to_string())),
            _ => None,
        })
        .collect();
    let mut prepared_files = Vec::new();
    let metadata_table = loaded.table.clone();
    let metadata_file_io = metadata_table.file_io().clone();
    let mut table_def = if matches!(
        metadata_table_type,
        Some(super::IcebergMetadataTableType::Partitions)
    ) {
        let snapshot_id = loaded.table.metadata().current_snapshot_id();
        let data_files = match snapshot_id {
            Some(snapshot_id) => extract_data_files_with_stats_at(&loaded.table, snapshot_id)
                .map_err(map_iceberg_error)?,
            None => Vec::new(),
        };
        prepared_files = data_files
            .iter()
            .cloned()
            .map(super::catalog::backend::data_file_with_stats_to_iceberg_data_file_info)
            .collect();
        super::catalog::backend::build_iceberg_table_def_with_files(
            entry, catalog, namespace, table, loaded, data_files,
        )
        .map_err(map_iceberg_error)?
    } else {
        super::catalog::backend::build_iceberg_schema_table_def_from_loaded(
            entry, catalog, namespace, table, loaded,
        )
        .map_err(map_iceberg_error)?
    };
    if matches!(
        metadata_table_type,
        Some(
            super::IcebergMetadataTableType::Files
                | super::IcebergMetadataTableType::Manifests
                | super::IcebergMetadataTableType::LogicalIcebergMetadata
        )
    ) {
        let rows = super::catalog::registry::block_on_iceberg(async {
            crate::connector::iceberg::metadata_read::read_metadata_table_rows(
                &metadata_table,
                &metadata_file_io,
                metadata_table_type
                    .clone()
                    .expect("metadata table type is present"),
            )
            .await
        })
        .map_err(map_iceberg_error)?
        .map_err(map_iceberg_error)?;
        if let crate::sql::planner::table::ScanSource::IcebergDataFiles { table, .. } =
            &mut table_def.source
        {
            table.serialized_metadata_rows = Some(rows);
        }
    }
    let hidden_columns = super::catalog::backend::hidden_internal_column_names_from_metadata(
        metadata_table.metadata(),
    );
    let metadata_columns = table_def
        .iceberg_row_lineage_metadata_columns
        .iter()
        .map(|column| column.name.clone())
        .collect();
    let (table_info, source_files) = match table_def.source {
        crate::sql::planner::table::ScanSource::IcebergDataFiles { table, files, .. } => {
            (table, files)
        }
        _ => {
            return Err(internal(
                "Iceberg metadata capability produced a non-Iceberg table source".to_string(),
            ));
        }
    };
    if prepared_files.is_empty() {
        prepared_files = source_files;
    }
    Ok(TablePayload {
        namespace: namespace.to_string(),
        table: table.to_string(),
        table_info: Some(table_info),
        metadata_columns,
        metadata_table_type,
        prepared_files,
        explicit_files: None,
        logical_type_columns,
        hidden_columns,
    })
}

#[derive(Clone, Deserialize, Serialize)]
struct TablePayload {
    namespace: String,
    table: String,
    table_info: Option<super::scan_model::IcebergTableInfo>,
    metadata_columns: Vec<String>,
    metadata_table_type: Option<super::IcebergMetadataTableType>,
    prepared_files: Vec<IcebergDataFileInfo>,
    explicit_files: Option<Vec<IcebergDataFileInfo>>,
    #[serde(default)]
    logical_type_columns: BTreeMap<String, String>,
    #[serde(default)]
    hidden_columns: Vec<String>,
}

#[derive(Clone, Deserialize, Serialize)]
struct ScanPayload {
    table: TablePayload,
    snapshot_id: Option<i64>,
    #[serde(default)]
    table_uuid: Option<String>,
    projection: Vec<usize>,
    limit: Option<u64>,
}

#[derive(Deserialize, Serialize)]
struct SplitPayload {
    #[serde(default = "default_iceberg_split_version")]
    version: u16,
    #[serde(default)]
    owner_instance_id: String,
    #[serde(default)]
    incarnation: [u8; 16],
    namespace: String,
    table: String,
    snapshot_id: Option<i64>,
    #[serde(default)]
    table_uuid: Option<String>,
    #[serde(default)]
    schema_id: Option<i32>,
    data_file: IcebergDataFileInfo,
    projection: Vec<usize>,
    limit: Option<u64>,
    #[serde(default)]
    delta: Option<IcebergDeltaSplitPayload>,
}

const ICEBERG_SPLIT_V1: u16 = 1;

fn default_iceberg_split_version() -> u16 {
    ICEBERG_SPLIT_V1
}

pub(crate) fn load_schema_table_def(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: novarocks_spi::connector::ConnectorRequestContext,
    catalog: &str,
    namespace: &str,
    table: &str,
) -> Result<(crate::sql::planner::table::TableDef, Option<i32>), String> {
    load_table_def_at(controls, context, catalog, namespace, table, None, true)
}

pub(crate) fn load_table_def_at(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: novarocks_spi::connector::ConnectorRequestContext,
    catalog: &str,
    namespace: &str,
    table: &str,
    snapshot_id: Option<i64>,
    schema_only: bool,
) -> Result<(crate::sql::planner::table::TableDef, Option<i32>), String> {
    use novarocks_spi::connector::{
        ConnectorTableIdentity, ConnectorTableRequest, ConnectorTableResolution,
    };

    let instance_id = ConnectorInstanceId::parse(catalog).map_err(|error| error.to_string())?;
    let lease = controls
        .acquire_current(&instance_id)
        .map_err(|error| error.to_string())?;
    let metadata = lease
        .binding()
        .metadata()
        .load_table(ConnectorTableRequest {
            table: ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from(namespace),
                table: Arc::from(table),
            },
            resolution: ConnectorTableResolution::StrictBaseTable,
            context: context.clone(),
        })
        .map_err(|error| error.to_string())?;
    let payload: TablePayload = decode_payload(metadata.table.payload(), "table handle")
        .map_err(|error| error.to_string())?;
    let schema_id = metadata.version.as_ref().and_then(|version| {
        <[u8; 4]>::try_from(version.as_ref())
            .ok()
            .map(i32::from_le_bytes)
    });
    let mut table_info = payload
        .table_info
        .ok_or_else(|| "Iceberg SPI table metadata is missing its read descriptor".to_string())?;
    let binding = if snapshot_id.is_some() {
        table_info.current_snapshot_id = snapshot_id;
        super::scan_model::IcebergDataFileBinding::ExplicitFiles
    } else {
        super::scan_model::IcebergDataFileBinding::CurrentSnapshot
    };
    let files = if schema_only {
        Vec::new()
    } else {
        plan_scan_files(
            controls,
            context,
            &table_info,
            binding,
            &payload.prepared_files,
            &(0..metadata.schema.fields().len()).collect::<Vec<_>>(),
        )?
    };
    let columns = columns_from_metadata(&metadata.schema, &payload.logical_type_columns)
        .into_iter()
        .filter(|column| {
            !payload
                .hidden_columns
                .iter()
                .any(|hidden| column.name.eq_ignore_ascii_case(hidden))
        })
        .collect();
    let table_def = crate::sql::planner::table::TableDef {
        name: payload.table,
        columns,
        iceberg_row_lineage_metadata_columns: iceberg_metadata_columns(&payload.metadata_columns)?,
        source: crate::sql::planner::table::ScanSource::IcebergDataFiles {
            table: table_info,
            files,
            cloud_properties: BTreeMap::new(),
            binding,
        },
    };
    Ok((table_def, schema_id))
}

pub(crate) fn load_metadata_table_def(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: novarocks_spi::connector::ConnectorRequestContext,
    catalog: &str,
    namespace: &str,
    table: &str,
    metadata_table_type: super::IcebergMetadataTableType,
) -> Result<crate::sql::planner::table::TableDef, String> {
    use novarocks_spi::connector::{
        ConnectorTableIdentity, ConnectorTableRequest, ConnectorTableResolution,
    };
    let instance_id = ConnectorInstanceId::parse(catalog).map_err(|error| error.to_string())?;
    let lease = controls
        .acquire_current(&instance_id)
        .map_err(|error| error.to_string())?;
    let alias = format!("{table}${}", metadata_table_name(&metadata_table_type));
    let metadata = lease
        .binding()
        .metadata()
        .load_table(ConnectorTableRequest {
            table: ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from(namespace),
                table: Arc::from(alias),
            },
            resolution: ConnectorTableResolution::ProviderReadAlias,
            context,
        })
        .map_err(|error| error.to_string())?;
    table_def_from_metadata(metadata)
}

fn table_def_from_metadata(
    metadata: ConnectorTableMetadata,
) -> Result<crate::sql::planner::table::TableDef, String> {
    let payload: TablePayload = decode_payload(metadata.table.payload(), "table handle")
        .map_err(|error| error.to_string())?;
    let table_info = payload
        .table_info
        .ok_or_else(|| "Iceberg SPI table metadata is missing its read descriptor".to_string())?;
    Ok(crate::sql::planner::table::TableDef {
        name: payload.table,
        columns: columns_from_metadata(&metadata.schema, &payload.logical_type_columns),
        iceberg_row_lineage_metadata_columns: iceberg_metadata_columns(&payload.metadata_columns)?,
        source: crate::sql::planner::table::ScanSource::IcebergDataFiles {
            table: table_info,
            files: payload.prepared_files,
            cloud_properties: BTreeMap::new(),
            binding: super::scan_model::IcebergDataFileBinding::CurrentSnapshot,
        },
    })
}

fn columns_from_metadata(
    schema: &Schema,
    logical_type_columns: &BTreeMap<String, String>,
) -> Vec<novarocks_catalog::schema::ColumnDef> {
    schema
        .fields()
        .iter()
        .map(|field| {
            let logical_type = match logical_type_columns
                .get(&field.name().to_lowercase())
                .map(String::as_str)
            {
                Some("bitmap") => Some(SqlType::Bitmap),
                Some("hll") => Some(SqlType::Hll),
                _ => None,
            };
            novarocks_catalog::schema::ColumnDef {
                name: field.name().to_string(),
                data_type: field.data_type().clone(),
                nullable: field.is_nullable(),
                write_default: None,
                logical_type,
            }
        })
        .collect()
}

fn iceberg_metadata_columns(
    names: &[String],
) -> Result<Vec<novarocks_catalog::schema::ColumnDef>, String> {
    names
        .iter()
        .map(|name| {
            let (data_type, nullable) = match name.as_str() {
                "_file" => (arrow::datatypes::DataType::Utf8, false),
                "_pos" | "_row_id" | "_last_updated_sequence_number" => {
                    (arrow::datatypes::DataType::Int64, false)
                }
                other => return Err(format!("unknown Iceberg metadata column `{other}`")),
            };
            Ok(novarocks_catalog::schema::ColumnDef {
                name: name.clone(),
                data_type,
                nullable,
                write_default: None,
                logical_type: None,
            })
        })
        .collect()
}

fn metadata_table_name(metadata_type: &super::IcebergMetadataTableType) -> &'static str {
    match metadata_type {
        super::IcebergMetadataTableType::Files => "FILES",
        super::IcebergMetadataTableType::Manifests => "MANIFESTS",
        super::IcebergMetadataTableType::LogicalIcebergMetadata => "LOGICAL_ICEBERG_METADATA",
        super::IcebergMetadataTableType::Snapshots => "SNAPSHOTS",
        super::IcebergMetadataTableType::History => "HISTORY",
        super::IcebergMetadataTableType::Refs => "REFS",
        super::IcebergMetadataTableType::Partitions => "PARTITIONS",
    }
}

pub(crate) fn plan_scan_files(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: novarocks_spi::connector::ConnectorRequestContext,
    table: &super::scan_model::IcebergTableInfo,
    binding: super::scan_model::IcebergDataFileBinding,
    explicit_files: &[IcebergDataFileInfo],
    projection: &[usize],
) -> Result<Vec<IcebergDataFileInfo>, String> {
    // Schema metadata does not carry data-file planning results.  A
    // time-travel table therefore has an ExplicitFiles binding with an empty
    // placeholder here; it must still ask the provider to enumerate the
    // table's already-pinned snapshot rather than treating that placeholder
    // as an explicitly empty scan.
    let file_override = (!matches!(
        binding,
        super::scan_model::IcebergDataFileBinding::ExplicitFiles
    ) || !explicit_files.is_empty())
    .then_some(explicit_files);
    let planned = plan_native_iceberg_read_with_file_override(
        controls,
        context,
        table,
        binding,
        file_override,
        projection,
    )?;
    planned
        .splits
        .iter()
        .map(|split| {
            decode_payload::<SplitPayload>(split.payload(), "split")
                .map(|payload| payload.data_file)
                .map_err(|error| error.to_string())
        })
        .collect()
}

/// Fully plans an Iceberg read through its real connector instance.  The
/// returned scan handle and splits are provider-owned bytes; callers may only
/// schedule and carry them.  This is intentionally separate from range
/// planning so native execution never reconstructs an Iceberg file scan in
/// core.
pub(crate) fn plan_native_iceberg_read(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: novarocks_spi::connector::ConnectorRequestContext,
    table: &super::scan_model::IcebergTableInfo,
    binding: super::scan_model::IcebergDataFileBinding,
    explicit_files: &[IcebergDataFileInfo],
    projection: &[usize],
) -> Result<PlannedIcebergConnectorRead, String> {
    plan_native_iceberg_read_with_file_override(
        controls,
        context,
        table,
        binding,
        Some(explicit_files),
        projection,
    )
}

fn plan_native_iceberg_read_with_file_override(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: novarocks_spi::connector::ConnectorRequestContext,
    table: &super::scan_model::IcebergTableInfo,
    binding: super::scan_model::IcebergDataFileBinding,
    explicit_files: Option<&[IcebergDataFileInfo]>,
    projection: &[usize],
) -> Result<PlannedIcebergConnectorRead, String> {
    use std::num::NonZeroUsize;

    let instance_id =
        ConnectorInstanceId::parse(&table.catalog).map_err(|error| error.to_string())?;
    let lease = controls
        .acquire_current(&instance_id)
        .map_err(|error| error.to_string())?;
    let control_binding = lease.binding();
    let declaration = control_binding
        .execution_declaration(&context)
        .map_err(|error| error.to_string())?;
    // A fixed snapshot can arrive without a materialized file list when it
    // was loaded through the schema metadata path (notably the synthetic
    // time-travel table used by an MV full refresh). Keep an explicit list
    // authoritative when present, but otherwise let the connector resolve
    // the requested snapshot instead of treating the empty list as an empty
    // table.
    let use_explicit_files = matches!(
        binding,
        super::scan_model::IcebergDataFileBinding::ExplicitFiles
    ) && explicit_files.is_some_and(|files| !files.is_empty());
    let table_handle = ConnectorTableHandle::try_new(
        instance_id.clone(),
        encode_payload(
            &TablePayload {
                namespace: table.namespace.clone(),
                table: table.table.clone(),
                table_info: Some(table.clone()),
                metadata_columns: Vec::new(),
                metadata_table_type: None,
                prepared_files: Vec::new(),
                explicit_files: use_explicit_files
                    .then(|| explicit_files.expect("non-empty explicit files").to_vec()),
                logical_type_columns: BTreeMap::new(),
                hidden_columns: Vec::new(),
            },
            "table handle",
            context.max_handle_payload_bytes(),
        )
        .map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    let scan = control_binding
        .planning()
        .begin_scan(
            &table_handle,
            novarocks_spi::connector::ConnectorBeginScanRequest {
                projection: projection.to_vec(),
                selector: table
                    .current_snapshot_id
                    .filter(|_| {
                        matches!(
                            binding,
                            super::scan_model::IcebergDataFileBinding::ExplicitFiles
                        )
                    })
                    .map(ConnectorReadSelector::SnapshotId)
                    .unwrap_or(ConnectorReadSelector::Current),
                limit: None,
                batch: novarocks_spi::connector::ConnectorBatchBudget {
                    max_rows: NonZeroUsize::new(4096).expect("batch rows are nonzero"),
                    max_bytes: NonZeroUsize::new(
                        novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
                    )
                    .expect("batch bytes are nonzero"),
                },
                context: context.clone(),
            },
        )
        .map_err(|error| error.to_string())?;
    let splits = control_binding
        .planning()
        .plan_splits(
            &scan.handle,
            ConnectorSplitPlanningRequest {
                target_parallelism: NonZeroUsize::new(1).expect("parallelism is nonzero"),
                max_split_bytes: None,
                context,
            },
        )
        .map_err(|error| error.to_string())?;
    if splits
        .iter()
        .any(|split| split.owner() != &control_binding.descriptor().instance_id)
    {
        return Err("Iceberg connector planned a split for another instance".to_string());
    }
    Ok(PlannedIcebergConnectorRead {
        declaration,
        planning_lease: lease,
        scan,
        splits,
        batch: novarocks_spi::connector::ConnectorBatchBudget {
            max_rows: NonZeroUsize::new(4096).expect("batch rows are nonzero"),
            max_bytes: NonZeroUsize::new(
                novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            )
            .expect("batch bytes are nonzero"),
        },
    })
}

/// Plan snapshot-delta physical reads as ordinary opaque Iceberg connector
/// splits.  Delta retains its logical planner identity; no native carrier or
/// core scan operator receives a role-specific file/deletion payload.
pub(crate) fn plan_native_iceberg_delta_read(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: novarocks_spi::connector::ConnectorRequestContext,
    table: &super::scan_model::IcebergTableInfo,
    sources: &[super::delta::DeltaSourceFile],
    delete_side: Option<&super::delta::DeltaScanDeleteSide>,
) -> Result<PlannedIcebergConnectorRead, String> {
    let mut planned = plan_native_iceberg_read(
        controls,
        context.clone(),
        table,
        super::scan_model::IcebergDataFileBinding::ExplicitFiles,
        &[],
        &[],
    )?;
    let owner = planned.declaration.descriptor().instance_id.clone();
    let incarnation = planned.declaration.incarnation().to_bytes();
    let mut total_payload_bytes = 0usize;
    let mut splits = Vec::with_capacity(sources.len());
    for (index, source) in sources.iter().cloned().enumerate() {
        let data_file = IcebergDataFileInfo {
            path: source.path.clone(),
            size: source.size,
            row_count: None,
            column_stats: None,
            partition_spec_id: source.partition_spec_id,
            partition_key: source.partition_key.clone(),
            first_row_id: source.first_row_id,
            data_sequence_number: source.data_sequence_number,
            ivm_change_op: None,
            included_positions: None,
            delete_files: Vec::new(),
            manifest_path: None,
            partition_values: Vec::new(),
        };
        let payload = SplitPayload {
            version: ICEBERG_SPLIT_V1,
            owner_instance_id: owner.as_str().to_string(),
            incarnation,
            namespace: table.namespace.clone(),
            table: table.table.clone(),
            snapshot_id: table.current_snapshot_id,
            table_uuid: table.table_uuid.clone(),
            schema_id: Some(table.schema_id),
            data_file,
            projection: Vec::new(),
            limit: None,
            delta: Some(IcebergDeltaSplitPayload {
                source,
                delete_side: delete_side.cloned(),
            }),
        };
        push_split_with_budget(
            &mut splits,
            &mut total_payload_bytes,
            owner.clone(),
            format!("delta-{index}"),
            &payload,
            u64::try_from(payload.data_file.size).ok(),
            &context,
        )
        .map_err(|error| error.to_string())?;
    }
    planned.splits = splits;
    Ok(planned)
}

pub(crate) struct PlannedIcebergConnectorRead {
    pub(crate) declaration: ConnectorExecutionDeclaration,
    pub(crate) planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    pub(crate) scan: ConnectorScan,
    pub(crate) splits: Vec<ConnectorSplit>,
    pub(crate) batch: novarocks_spi::connector::ConnectorBatchBudget,
}

fn ensure_owner(
    owner: &ConnectorInstanceId,
    expected: &ConnectorInstanceId,
) -> Result<(), ConnectorError> {
    if owner == expected {
        Ok(())
    } else {
        Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector handle belongs to a different instance",
        ))
    }
}

#[allow(clippy::too_many_arguments)]
fn push_split_with_budget(
    splits: &mut Vec<ConnectorSplit>,
    total_payload_bytes: &mut usize,
    owner: ConnectorInstanceId,
    split_id: String,
    payload: &SplitPayload,
    estimated_bytes: Option<u64>,
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), ConnectorError> {
    // Encoding one candidate is bounded by the per-handle budget. Admit its
    // bytes against the aggregate budget before allocating/pushing the opaque
    // split, so a rejected high-cardinality plan never builds the full split
    // vector transiently.
    let payload = encode_payload(payload, "split", context.max_handle_payload_bytes())?;
    let next_total = total_payload_bytes
        .checked_add(payload.len())
        .filter(|total| *total <= context.max_total_payload_bytes())
        .ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "Iceberg split payloads exceed the request budget",
            )
        })?;
    let split = ConnectorSplit::try_new(owner, split_id, payload, estimated_bytes)?;
    splits.push(split);
    *total_payload_bytes = next_total;
    Ok(())
}

fn encode_payload(
    payload: &impl Serialize,
    subject: &str,
    max_payload_bytes: usize,
) -> Result<Bytes, ConnectorError> {
    serde_json::to_vec(payload)
        .map_err(|error| internal(format!("serialize Iceberg {subject}: {error}")))
        .and_then(|payload| {
            if payload.len() > max_payload_bytes {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    format!("Iceberg {subject} exceeds the request payload budget"),
                ));
            }
            Ok(Bytes::from(payload))
        })
}

fn decode_payload<T: for<'de> Deserialize<'de>>(
    payload: &Bytes,
    subject: &str,
) -> Result<T, ConnectorError> {
    serde_json::from_slice(payload).map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            format!("decode Iceberg {subject}: {error}"),
        )
    })
}

fn map_iceberg_error(error: String) -> ConnectorError {
    let kind = if error.contains("not found") || error.contains("does not exist") {
        ConnectorErrorKind::NotFound
    } else {
        ConnectorErrorKind::Internal
    };
    ConnectorError::new(kind, error)
}

fn internal(message: String) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, message)
}

#[cfg(test)]
pub(crate) fn replace_split_path_for_test(
    split: &ConnectorSplit,
    path: &str,
) -> Result<ConnectorSplit, ConnectorError> {
    let mut payload: SplitPayload = decode_payload(split.payload(), "split")?;
    payload.data_file.path = path.to_string();
    ConnectorSplit::try_new(
        split.owner().clone(),
        split.split_id(),
        encode_payload(
            &payload,
            "split",
            novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        )?,
        split.estimated_bytes(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::iceberg::scan_model::{IcebergSchemaDef, IcebergTableInfo};

    struct NotCancelled;

    impl novarocks_spi::connector::ConnectorCancellation for NotCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn context_with_payload_budgets(
        max_handle_payload_bytes: usize,
        max_total_payload_bytes: usize,
    ) -> novarocks_spi::connector::ConnectorRequestContext {
        novarocks_spi::connector::ConnectorRequestContext::try_new(
            Instant::now() + std::time::Duration::from_secs(30),
            Arc::new(NotCancelled),
            max_handle_payload_bytes,
            max_total_payload_bytes,
        )
        .expect("connector request context")
    }

    #[test]
    fn snapshot_membership_cache_is_bounded_and_reloads_evicted_snapshot() {
        fn key(snapshot_id: i64) -> SnapshotMembershipKey {
            SnapshotMembershipKey {
                namespace: "db".to_string(),
                table: "orders".to_string(),
                table_uuid: "table-uuid".to_string(),
                snapshot_id,
            }
        }

        let cache = SnapshotMembershipCache::new(1);
        let loads = std::sync::atomic::AtomicUsize::new(0);
        let membership = || {
            loads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(Arc::new(HashSet::new()))
        };

        cache
            .get_or_try_init(key(1), membership)
            .expect("load first snapshot");
        cache
            .get_or_try_init(key(1), membership)
            .expect("reuse first snapshot");
        assert_eq!(loads.load(std::sync::atomic::Ordering::SeqCst), 1);

        cache
            .get_or_try_init(key(2), membership)
            .expect("load second snapshot");
        cache
            .get_or_try_init(key(1), membership)
            .expect("reload evicted first snapshot");

        assert_eq!(loads.load(std::sync::atomic::Ordering::SeqCst), 3);
        assert_eq!(
            cache.state.lock().expect("cache state").entries.len(),
            1,
            "cache must never retain more snapshots than its configured capacity"
        );
    }

    #[test]
    fn split_payload_does_not_repeat_serialized_table_metadata() {
        let table = TablePayload {
            namespace: "db".to_string(),
            table: "orders".to_string(),
            table_info: Some(IcebergTableInfo {
                catalog: "ice".to_string(),
                namespace: "db".to_string(),
                table: "orders".to_string(),
                table_uuid: None,
                current_snapshot_id: Some(7),
                schema_id: 1,
                location: "s3://warehouse/db/orders".to_string(),
                schema: IcebergSchemaDef { fields: Vec::new() },
                serialized_metadata: Some("x".repeat(256 * 1024)),
                serialized_metadata_rows: None,
            }),
            metadata_columns: Vec::new(),
            metadata_table_type: None,
            prepared_files: Vec::new(),
            explicit_files: None,
            logical_type_columns: BTreeMap::new(),
            hidden_columns: Vec::new(),
        };
        let payload = SplitPayload {
            version: ICEBERG_SPLIT_V1,
            owner_instance_id: "ice".to_string(),
            incarnation: [0; 16],
            namespace: table.namespace,
            table: table.table,
            snapshot_id: Some(7),
            table_uuid: Some("table-uuid".to_string()),
            schema_id: Some(1),
            data_file: IcebergDataFileInfo::for_test(
                "s3://warehouse/db/orders/data-1.parquet",
                1024,
                10,
            ),
            projection: vec![0],
            limit: None,
            delta: None,
        };

        let encoded = serde_json::to_vec(&payload).expect("encode split payload");
        assert!(
            encoded.len() < 4096,
            "split payload repeated table metadata: {} bytes per split",
            encoded.len()
        );
        assert!(
            encoded.len().saturating_mul(512)
                <= novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
            "ordinary 512-split planning exceeds the total payload budget"
        );
    }

    #[test]
    fn aggregate_budget_rejects_candidate_before_split_is_pushed() {
        let owner = ConnectorInstanceId::parse("ice").expect("owner");
        let payload = |suffix: &str| SplitPayload {
            version: ICEBERG_SPLIT_V1,
            owner_instance_id: "ice".to_string(),
            incarnation: [0; 16],
            namespace: "db".to_string(),
            table: "orders".to_string(),
            snapshot_id: Some(7),
            table_uuid: Some("table-uuid".to_string()),
            schema_id: Some(1),
            data_file: IcebergDataFileInfo::for_test(
                &format!(
                    "s3://warehouse/db/orders/{}-{suffix}.parquet",
                    "x".repeat(512)
                ),
                1024,
                10,
            ),
            projection: vec![0],
            limit: None,
            delta: None,
        };
        let first = payload("first");
        let second = payload("second");
        let first_len = serde_json::to_vec(&first).expect("encode first").len();
        let second_len = serde_json::to_vec(&second).expect("encode second").len();
        let context =
            context_with_payload_budgets(first_len.max(second_len), first_len + second_len - 1);
        let mut splits = Vec::new();
        let mut total = 0;

        push_split_with_budget(
            &mut splits,
            &mut total,
            owner.clone(),
            "first".to_string(),
            &first,
            Some(1024),
            &context,
        )
        .expect("first split fits");
        let admitted_total = total;
        let error = push_split_with_budget(
            &mut splits,
            &mut total,
            owner,
            "second".to_string(),
            &second,
            Some(1024),
            &context,
        )
        .expect_err("second split must exceed aggregate budget");

        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
        assert_eq!(splits.len(), 1, "rejected split must not be pushed");
        assert_eq!(total, admitted_total, "rejection must not consume budget");
    }

    #[test]
    fn compat_splits_are_owned_by_the_startup_iceberg_instance() {
        let instance = compose_compat_execution_binding(
            IcebergReadBinding::default_binding(None).expect("compose compat Iceberg binding"),
        )
        .expect("compose compat Iceberg instance");
        assert_eq!(
            instance.key().instance_id.as_str(),
            COMPAT_ICEBERG_INSTANCE_ID
        );
        assert_eq!(instance.provider_id().as_str(), PROVIDER_ID);

        let splits = build_compat_read_splits(vec![IcebergDataFileInfo::for_test(
            "file:///tmp/compat.parquet",
            64,
            2,
        )])
        .expect("encode compat split");
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].owner().as_str(), COMPAT_ICEBERG_INSTANCE_ID);
        let payload: SplitPayload =
            decode_payload(splits[0].payload(), "compat split").expect("decode provider split");
        assert_eq!(payload.incarnation, COMPAT_ICEBERG_INCARNATION);
        assert_eq!(payload.data_file.path, "file:///tmp/compat.parquet");
    }

    #[test]
    fn compat_delta_splits_keep_delete_facts_in_the_provider_payload() {
        let source = super::super::delta::DeltaSourceFile {
            path: "file:///tmp/compat-delta.parquet".to_string(),
            size: 64,
            role: super::super::delta::DeltaSourceRole::DataFile,
            partition_spec_id: None,
            partition_key: None,
            first_row_id: Some(10),
            data_sequence_number: Some(20),
            row_id_allow_list: None,
        };
        let splits = build_compat_delta_read_splits(vec![source.clone()], None)
            .expect("encode compat delta split");
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].owner().as_str(), COMPAT_ICEBERG_INSTANCE_ID);
        let payload: SplitPayload =
            decode_payload(splits[0].payload(), "compat delta split").expect("decode split");
        assert_eq!(payload.incarnation, COMPAT_ICEBERG_INCARNATION);
        assert_eq!(payload.delta.expect("delta payload").source, source);
    }

    #[test]
    fn plan_splits_enforces_aggregate_budget_incrementally() {
        let instance_id = ConnectorInstanceId::parse("ice").expect("instance ID");
        let files = ["first", "second"]
            .into_iter()
            .map(|suffix| {
                IcebergDataFileInfo::for_test(
                    &format!(
                        "s3://warehouse/db/orders/{}-{suffix}.parquet",
                        "x".repeat(512)
                    ),
                    1024,
                    10,
                )
            })
            .collect::<Vec<_>>();
        let split_payloads = files
            .iter()
            .cloned()
            .map(|data_file| SplitPayload {
                version: ICEBERG_SPLIT_V1,
                owner_instance_id: instance_id.as_str().to_string(),
                incarnation: [0; 16],
                namespace: "db".to_string(),
                table: "orders".to_string(),
                snapshot_id: None,
                table_uuid: None,
                schema_id: None,
                data_file,
                projection: vec![0],
                limit: None,
                delta: None,
            })
            .collect::<Vec<_>>();
        let lengths = split_payloads
            .iter()
            .map(|payload| serde_json::to_vec(payload).expect("encode split").len())
            .collect::<Vec<_>>();
        let context = context_with_payload_budgets(
            *lengths.iter().max().expect("split length"),
            lengths.iter().sum::<usize>() - 1,
        );
        let scan = ConnectorScanHandle::try_new(
            instance_id.clone(),
            encode_payload(
                &ScanPayload {
                    table: TablePayload {
                        namespace: "db".to_string(),
                        table: "orders".to_string(),
                        table_info: None,
                        metadata_columns: Vec::new(),
                        metadata_table_type: None,
                        prepared_files: Vec::new(),
                        explicit_files: Some(files),
                        logical_type_columns: BTreeMap::new(),
                        hidden_columns: Vec::new(),
                    },
                    snapshot_id: None,
                    table_uuid: None,
                    projection: vec![0],
                    limit: None,
                },
                "scan handle",
                novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            )
            .expect("scan payload"),
        )
        .expect("scan handle");
        let provider = IcebergControlProvider {
            instance_id,
            incarnation: ConnectorInstanceIncarnation::from_bytes([0; 16]),
            registry: Arc::new(RwLock::new(IcebergCatalogRegistry::default())),
            snapshot_memberships: Arc::new(SnapshotMembershipCache::new(
                MAX_CACHED_SNAPSHOT_MEMBERSHIPS,
            )),
        };

        let error = ConnectorScanPlanning::plan_splits(
            &provider,
            &scan,
            ConnectorSplitPlanningRequest {
                target_parallelism: std::num::NonZeroUsize::new(1).expect("parallelism"),
                max_split_bytes: None,
                context,
            },
        )
        .expect_err("aggregate split budget must reject the plan");

        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
    }
}

#[cfg(test)]
pub(crate) fn register_planned_files_fixture(
    registry: &crate::connector::ConnectorRegistry,
    catalog: &str,
    files: Vec<IcebergDataFileInfo>,
    seen_projections: Option<Arc<std::sync::Mutex<Vec<Vec<usize>>>>>,
) {
    register_planned_table_files_fixture(
        registry,
        catalog,
        std::collections::HashMap::from([("*".to_string(), files)]),
        seen_projections,
    );
}

#[cfg(test)]
pub(crate) fn register_planned_table_files_fixture(
    registry: &crate::connector::ConnectorRegistry,
    catalog: &str,
    files_by_table: std::collections::HashMap<String, Vec<IcebergDataFileInfo>>,
    seen_projections: Option<Arc<std::sync::Mutex<Vec<Vec<usize>>>>>,
) {
    struct Fixture {
        instance_id: ConnectorInstanceId,
        files_by_table: std::collections::HashMap<String, Vec<IcebergDataFileInfo>>,
        seen_projections: Option<Arc<std::sync::Mutex<Vec<Vec<usize>>>>>,
    }

    impl ConnectorScanPlanning for Fixture {
        fn instance_id(&self) -> &ConnectorInstanceId {
            &self.instance_id
        }

        fn begin_scan(
            &self,
            table: &ConnectorTableHandle,
            request: ConnectorBeginScanRequest,
        ) -> Result<ConnectorScan, ConnectorError> {
            if request.context.cancellation().is_cancelled() {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Cancelled,
                    "planned-files fixture observed caller cancellation",
                ));
            }
            let table: TablePayload = decode_payload(table.payload(), "fixture table handle")?;
            if let Some(seen) = &self.seen_projections {
                seen.lock()
                    .expect("fixture projection lock")
                    .push(request.projection.clone());
            }
            Ok(ConnectorScan {
                handle: ConnectorScanHandle::try_new(
                    self.instance_id.clone(),
                    encode_payload(
                        &ScanPayload {
                            table,
                            snapshot_id: None,
                            table_uuid: None,
                            projection: request.projection,
                            limit: request.limit,
                        },
                        "fixture scan handle",
                        request.context.max_handle_payload_bytes(),
                    )?,
                )?,
                output_schema: Arc::new(Schema::empty()),
            })
        }

        fn plan_splits(
            &self,
            scan: &ConnectorScanHandle,
            request: ConnectorSplitPlanningRequest,
        ) -> Result<Vec<ConnectorSplit>, ConnectorError> {
            let scan: ScanPayload = decode_payload(scan.payload(), "fixture scan handle")?;
            self.files_by_table
                .get(&scan.table.table)
                .or_else(|| self.files_by_table.get("*"))
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::NotFound,
                        format!("no planned files for fixture table {}", scan.table.table),
                    )
                })?
                .iter()
                .cloned()
                .enumerate()
                .map(|(index, data_file)| {
                    ConnectorSplit::try_new(
                        self.instance_id.clone(),
                        format!("fixture-{index}"),
                        encode_payload(
                            &SplitPayload {
                                version: ICEBERG_SPLIT_V1,
                                owner_instance_id: self.instance_id.as_str().to_string(),
                                incarnation: [0; 16],
                                namespace: scan.table.namespace.clone(),
                                table: scan.table.table.clone(),
                                snapshot_id: None,
                                table_uuid: None,
                                schema_id: None,
                                data_file,
                                projection: scan.projection.clone(),
                                limit: scan.limit,
                                delta: None,
                            },
                            "fixture split",
                            request.context.max_handle_payload_bytes(),
                        )?,
                        None,
                    )
                })
                .collect()
        }
    }

    impl ConnectorMetadata for Fixture {
        fn instance_id(&self) -> &ConnectorInstanceId {
            &self.instance_id
        }

        fn namespace_exists(&self, _: ConnectorNamespaceRequest) -> Result<bool, ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "planned-files fixture does not implement metadata",
            ))
        }

        fn table_exists(&self, _: ConnectorTableRequest) -> Result<bool, ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "planned-files fixture does not implement metadata",
            ))
        }

        fn list_tables(
            &self,
            _: ConnectorListTablesRequest,
        ) -> Result<Vec<novarocks_spi::connector::ConnectorTableIdentity>, ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "planned-files fixture does not implement metadata",
            ))
        }

        fn load_table(
            &self,
            _: ConnectorTableRequest,
        ) -> Result<ConnectorTableMetadata, ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "planned-files fixture does not implement metadata",
            ))
        }
    }

    let instance_id = ConnectorInstanceId::parse(catalog).expect("fixture instance ID");
    let read = Arc::new(Fixture {
        instance_id: instance_id.clone(),
        files_by_table,
        seen_projections,
    });
    let descriptor = ConnectorInstanceDescriptor {
        provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
            .expect("fixture provider ID"),
        instance_id,
    };
    let incarnation = ConnectorInstanceIncarnation::from_bytes([0; 16]);
    registry.register_fixture_control(
        ConnectorControlBinding::try_new(
            descriptor.clone(),
            incarnation,
            read.clone(),
            read,
            Arc::new(IcebergInstanceDistribution {
                descriptor,
                incarnation,
            }),
        )
        .expect("fixture connector control binding"),
    );
}
