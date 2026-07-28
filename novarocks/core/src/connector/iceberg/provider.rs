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

use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use arrow::datatypes::{Schema, SchemaRef};
use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorBatchReader, ConnectorBeginScanRequest, ConnectorError, ConnectorErrorKind,
    ConnectorInstance, ConnectorInstanceDescriptor, ConnectorInstanceId,
    ConnectorListTablesRequest, ConnectorMetadata, ConnectorNamespaceRequest, ConnectorRead,
    ConnectorReadSelector, ConnectorScan, ConnectorScanHandle, ConnectorSplit,
    ConnectorSplitPlanningRequest, ConnectorTableHandle, ConnectorTableMetadata,
    ConnectorTableRequest,
};
use serde::{Deserialize, Serialize};

use super::catalog::IcebergCatalogEntry;
use super::catalog::registry::{
    IcebergCatalogRegistry, extract_data_files_with_stats_at, list_tables, load_table,
};
use super::scan_model::IcebergDataFileInfo;
use super::scan_range::plan_iceberg_file_ranges;
use crate::cache::{CacheOptions, DataCacheContext};
use crate::common::ids::SlotId;
use crate::connector::HdfsScanConfig;
use crate::connector::hdfs::HdfsFileBatchReader;
use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
use crate::formats::FileFormatConfig;
use crate::formats::parquet::{ParquetReadCachePolicy, ParquetScanConfig, ParquetSlotKind};

const PROVIDER_ID: &str = "iceberg";

#[derive(Clone)]
pub(crate) struct IcebergConnectorInstance {
    instance_id: ConnectorInstanceId,
    registry: Arc<RwLock<IcebergCatalogRegistry>>,
}

impl IcebergConnectorInstance {
    pub(crate) fn new(
        instance_id: ConnectorInstanceId,
        registry: Arc<RwLock<IcebergCatalogRegistry>>,
    ) -> Result<ConnectorInstance, ConnectorError> {
        let provider = Arc::new(Self {
            instance_id: instance_id.clone(),
            registry,
        });
        ConnectorInstance::try_new(
            ConnectorInstanceDescriptor {
                provider_id: novarocks_spi::connector::ConnectorProviderId::parse(PROVIDER_ID)?,
                instance_id,
            },
            Some(provider.clone()),
            provider,
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
        table: &TablePayload,
        projection: &[usize],
    ) -> Result<SchemaRef, ConnectorError> {
        let loaded =
            load_table(entry, &table.namespace, &table.table).map_err(map_iceberg_error)?;
        let schema =
            iceberg::arrow::schema_to_arrow_schema(loaded.table.metadata().current_schema())
                .map_err(|error| internal(format!("convert Iceberg schema to Arrow: {error}")))?;
        let fields = projection
            .iter()
            .map(|index| {
                schema.fields().get(*index).cloned().ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        format!("Iceberg projection index {index} is outside the table schema"),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Arc::new(Schema::new(fields)))
    }

    fn select_snapshot(
        &self,
        entry: &IcebergCatalogEntry,
        table: &TablePayload,
        selector: ConnectorReadSelector,
    ) -> Result<Option<i64>, ConnectorError> {
        let loaded =
            load_table(entry, &table.namespace, &table.table).map_err(map_iceberg_error)?;
        let metadata = loaded.table.metadata();
        match selector {
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
        }
    }

    fn chunk_schema_for(&self, schema: &SchemaRef) -> Result<Arc<ChunkSchema>, ConnectorError> {
        let slots = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let slot_id = u32::try_from(index + 1).map_err(|_| {
                    ConnectorError::new(
                        ConnectorErrorKind::ResourceExhausted,
                        "Iceberg projection has too many columns",
                    )
                })?;
                Ok(ChunkSlotSchema::new_with_field(
                    SlotId::new(slot_id),
                    field.as_ref().clone(),
                    None,
                    None,
                ))
            })
            .collect::<Result<Vec<_>, ConnectorError>>()?;
        ChunkSchema::try_new(slots)
            .map(Arc::new)
            .map_err(|error| internal(format!("build Iceberg chunk schema: {error}")))
    }
}

impl ConnectorMetadata for IcebergConnectorInstance {
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
        let table = TablePayload {
            namespace: request.table.namespace.to_string(),
            table: request.table.table.to_string(),
            explicit_files: None,
        };
        let loaded =
            load_table(&entry, &table.namespace, &table.table).map_err(map_iceberg_error)?;
        let schema = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(loaded.table.metadata().current_schema())
                .map_err(|error| internal(format!("convert Iceberg schema to Arrow: {error}")))?,
        );
        let version = Some(Bytes::copy_from_slice(
            &loaded.table.metadata().current_schema_id().to_le_bytes(),
        ));
        Ok(ConnectorTableMetadata {
            identity: request.table,
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

impl ConnectorRead for IcebergConnectorInstance {
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
        let output_schema = self.schema_for(&entry, &table, &request.projection)?;
        let snapshot_id = if table.explicit_files.is_some() {
            None
        } else {
            self.select_snapshot(&entry, &table, request.selector)?
        };
        let payload = ScanPayload {
            table,
            snapshot_id,
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
                extract_data_files_with_stats_at(&loaded.table, snapshot_id)
                    .map_err(map_iceberg_error)?
                    .into_iter()
                    .map(super::catalog::backend::data_file_with_stats_to_iceberg_data_file_info)
                    .collect()
            }
        };
        let mut remaining = scan.limit;
        let splits = files
            .into_iter()
            .enumerate()
            .map(|(index, file)| {
                if let Some(remaining_rows) = remaining.as_mut() {
                    if *remaining_rows == 0 {
                        return Ok(None);
                    }
                    if let Some(row_count) =
                        file.row_count.and_then(|count| u64::try_from(count).ok())
                    {
                        *remaining_rows = remaining_rows.saturating_sub(row_count);
                    }
                }
                let estimated_bytes = u64::try_from(file.size).ok();
                let payload = SplitPayload {
                    table: scan.table.clone(),
                    snapshot_id: scan.snapshot_id,
                    data_file: file,
                    projection: scan.projection.clone(),
                    limit: scan.limit,
                };
                Ok(Some(ConnectorSplit::try_new(
                    self.instance_id.clone(),
                    format!(
                        "{}-{index}",
                        scan.snapshot_id
                            .map(|snapshot_id| snapshot_id.to_string())
                            .unwrap_or_else(|| "explicit".to_string())
                    ),
                    encode_payload(
                        &payload,
                        "split",
                        request.context.max_handle_payload_bytes(),
                    )?,
                    estimated_bytes,
                )?))
            })
            .filter_map(Result::transpose)
            .collect::<Result<Vec<_>, ConnectorError>>()?;
        let total_payload_bytes = splits
            .iter()
            .map(|split| split.payload().len())
            .sum::<usize>();
        if total_payload_bytes > request.context.max_total_payload_bytes() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "Iceberg split payloads exceed the request budget",
            ));
        }
        Ok(splits)
    }

    fn open_reader(
        &self,
        split: &ConnectorSplit,
        request: novarocks_spi::connector::ConnectorOpenReaderRequest,
    ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
        self.validate_context(&request.context)?;
        ensure_owner(split.owner(), &self.instance_id)?;
        let split: SplitPayload = decode_payload(split.payload(), "split")?;
        let entry = self.entry(self.instance_id.as_str())?;
        let output_schema = self.schema_for(&entry, &split.table, &split.projection)?;
        if output_schema.as_ref() != request.expected_schema.as_ref() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg reader expected schema does not match its scan projection",
            ));
        }
        let loaded = load_table(&entry, &split.table.namespace, &split.table.table)
            .map_err(map_iceberg_error)?;
        if let Some(snapshot_id) = split.snapshot_id {
            let present = extract_data_files_with_stats_at(&loaded.table, snapshot_id)
                .map_err(map_iceberg_error)?
                .iter()
                .any(|file| file.path == split.data_file.path);
            if !present {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::NotFound,
                    "Iceberg split data file is absent from its pinned snapshot",
                ));
            }
        }
        let ranges = plan_iceberg_file_ranges(&split.data_file).map_err(map_iceberg_error)?;
        let chunk_schema = self.chunk_schema_for(&output_schema)?;
        let columns = output_schema
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect::<Vec<_>>();
        let batch_size = request.batch.max_rows.get();
        let parquet = ParquetScanConfig {
            columns,
            chunk_schema: Arc::clone(&chunk_schema),
            slot_kinds: vec![ParquetSlotKind::Regular; chunk_schema.slots().len()],
            case_sensitive: true,
            enable_page_index: false,
            min_max_predicates: Vec::new(),
            runtime_min_max_filter_columns: Default::default(),
            variant_path_predicates: Vec::new(),
            batch_size: Some(batch_size),
            datacache: DataCacheContext::external(
                CacheOptions::from_query_options(None)
                    .map_err(|error| internal(format!("default cache options: {error}")))?,
            ),
            cache_policy: ParquetReadCachePolicy::with_flags(false, false, None),
            profile_label: Some("spi_iceberg_reader".to_string()),
            iceberg_output_schema: Some(Arc::clone(&output_schema)),
            variant_path_columns: Vec::new(),
            query_global_dicts: Default::default(),
        };
        Ok(Box::new(HdfsFileBatchReader::new(
            HdfsScanConfig {
                original_range_count: ranges.len(),
                ranges,
                has_more: false,
                limit: split.limit.and_then(|limit| usize::try_from(limit).ok()),
                profile_label: Some("spi_iceberg_reader".to_string()),
                format: Some(FileFormatConfig::Parquet(parquet)),
                object_store_config: loaded.object_store_config,
                iceberg_table_locations: Default::default(),
                query_global_dicts: Default::default(),
                iceberg_runtime_pruning: None,
            },
            request.context,
            request.batch.max_rows.get(),
            request.batch.max_bytes.get(),
        )))
    }
}

#[derive(Clone, Deserialize, Serialize)]
struct TablePayload {
    namespace: String,
    table: String,
    explicit_files: Option<Vec<IcebergDataFileInfo>>,
}

#[derive(Clone, Deserialize, Serialize)]
struct ScanPayload {
    table: TablePayload,
    snapshot_id: Option<i64>,
    projection: Vec<usize>,
    limit: Option<u64>,
}

#[derive(Deserialize, Serialize)]
struct SplitPayload {
    table: TablePayload,
    snapshot_id: Option<i64>,
    data_file: IcebergDataFileInfo,
    projection: Vec<usize>,
    limit: Option<u64>,
}

struct NeverCancelled;

impl novarocks_spi::connector::ConnectorCancellation for NeverCancelled {
    fn is_cancelled(&self) -> bool {
        false
    }
}

fn metadata_context() -> Result<novarocks_spi::connector::ConnectorRequestContext, String> {
    novarocks_spi::connector::ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(60),
        Arc::new(NeverCancelled),
        novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .map_err(|error| error.to_string())
}

pub(crate) fn load_schema_table_def(
    connectors: &crate::connector::ConnectorRegistry,
    registry: &Arc<RwLock<IcebergCatalogRegistry>>,
    catalog: &str,
    namespace: &str,
    table: &str,
) -> Result<(crate::sql::planner::table::TableDef, Option<i32>), String> {
    load_table_def_at(connectors, registry, catalog, namespace, table, None, true)
}

pub(crate) fn load_table_def_at(
    connectors: &crate::connector::ConnectorRegistry,
    registry: &Arc<RwLock<IcebergCatalogRegistry>>,
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
    let instance = connectors
        .connector_instance(&instance_id)
        .map_err(|error| error.to_string())?;
    let metadata = instance
        .metadata()
        .ok_or_else(|| format!("connector instance {catalog} has no metadata capability"))?
        .load_table(ConnectorTableRequest {
            table: ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from(namespace),
                table: Arc::from(table),
            },
            resolution: ConnectorTableResolution::StrictBaseTable,
            context: metadata_context()?,
        })
        .map_err(|error| error.to_string())?;
    let payload: TablePayload = decode_payload(metadata.table.payload(), "table handle")
        .map_err(|error| error.to_string())?;
    let resolved = crate::connector::backend::ResolvedTable {
        catalog: catalog.to_string(),
        namespace: payload.namespace,
        table: payload.table,
        columns: metadata
            .schema
            .fields()
            .iter()
            .map(|field| novarocks_catalog::schema::ColumnDef {
                name: field.name().to_string(),
                data_type: field.data_type().clone(),
                nullable: field.is_nullable(),
                write_default: None,
                logical_type: None,
            })
            .collect(),
    };
    let schema_id = metadata.version.as_ref().and_then(|version| {
        <[u8; 4]>::try_from(version.as_ref())
            .ok()
            .map(i32::from_le_bytes)
    });
    let table_def = if schema_only {
        super::catalog::resolve_iceberg_schema_table_def(registry, &resolved)
    } else {
        super::catalog::resolve_iceberg_table_def_at(registry, &resolved, snapshot_id)
    }?;
    Ok((table_def, schema_id))
}

pub(crate) fn load_metadata_table_def(
    connectors: &crate::connector::ConnectorRegistry,
    registry: &Arc<RwLock<IcebergCatalogRegistry>>,
    catalog: &str,
    namespace: &str,
    table: &str,
    metadata_table_type: super::IcebergMetadataTableType,
) -> Result<crate::sql::planner::table::TableDef, String> {
    let (base, _) =
        load_table_def_at(connectors, registry, catalog, namespace, table, None, false)?;
    if metadata_table_type == super::IcebergMetadataTableType::Partitions {
        return Ok(base);
    }
    if matches!(
        metadata_table_type,
        super::IcebergMetadataTableType::Files
            | super::IcebergMetadataTableType::Manifests
            | super::IcebergMetadataTableType::LogicalIcebergMetadata
    ) {
        let resolved = crate::connector::backend::ResolvedTable {
            catalog: catalog.to_string(),
            namespace: namespace.to_string(),
            table: table.to_string(),
            columns: base.columns,
        };
        return super::catalog::resolve_iceberg_metadata_rows_table_def(
            registry,
            &resolved,
            metadata_table_type,
        );
    }
    load_schema_table_def(connectors, registry, catalog, namespace, table)
        .map(|(table_def, _)| table_def)
}

pub(crate) fn plan_scan_files(
    connectors: &crate::connector::ConnectorRegistry,
    table: &super::scan_model::IcebergTableInfo,
    binding: super::scan_model::IcebergDataFileBinding,
    explicit_files: &[IcebergDataFileInfo],
    projection: &[usize],
) -> Result<Vec<IcebergDataFileInfo>, String> {
    use std::num::NonZeroUsize;

    let instance_id =
        ConnectorInstanceId::parse(&table.catalog).map_err(|error| error.to_string())?;
    let instance = connectors
        .connector_instance(&instance_id)
        .map_err(|error| error.to_string())?;
    let context = novarocks_spi::connector::ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(60),
        Arc::new(NeverCancelled),
        novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .map_err(|error| error.to_string())?;
    let table_handle = ConnectorTableHandle::try_new(
        instance_id,
        encode_payload(
            &TablePayload {
                namespace: table.namespace.clone(),
                table: table.table.clone(),
                explicit_files: matches!(
                    binding,
                    super::scan_model::IcebergDataFileBinding::ExplicitFiles
                )
                .then(|| explicit_files.to_vec()),
            },
            "table handle",
            context.max_handle_payload_bytes(),
        )
        .map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    let scan = instance
        .read()
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
    let splits = instance
        .read()
        .plan_splits(
            &scan.handle,
            ConnectorSplitPlanningRequest {
                target_parallelism: NonZeroUsize::new(1).expect("parallelism is nonzero"),
                max_split_bytes: None,
                context,
            },
        )
        .map_err(|error| error.to_string())?;
    splits
        .iter()
        .map(|split| {
            ensure_owner(split.owner(), &instance.descriptor().instance_id)
                .map_err(|error| error.to_string())?;
            decode_payload::<SplitPayload>(split.payload(), "split")
                .map(|payload| payload.data_file)
                .map_err(|error| error.to_string())
        })
        .collect()
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

    impl ConnectorRead for Fixture {
        fn instance_id(&self) -> &ConnectorInstanceId {
            &self.instance_id
        }

        fn begin_scan(
            &self,
            table: &ConnectorTableHandle,
            request: ConnectorBeginScanRequest,
        ) -> Result<ConnectorScan, ConnectorError> {
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
                                table: scan.table.clone(),
                                snapshot_id: None,
                                data_file,
                                projection: scan.projection.clone(),
                                limit: scan.limit,
                            },
                            "fixture split",
                            request.context.max_handle_payload_bytes(),
                        )?,
                        None,
                    )
                })
                .collect()
        }

        fn open_reader(
            &self,
            _: &ConnectorSplit,
            _: novarocks_spi::connector::ConnectorOpenReaderRequest,
        ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "planned-files fixture does not open readers",
            ))
        }
    }

    let instance_id = ConnectorInstanceId::parse(catalog).expect("fixture instance ID");
    let read = Arc::new(Fixture {
        instance_id: instance_id.clone(),
        files_by_table,
        seen_projections,
    });
    registry
        .register_connector_instance(
            ConnectorInstance::try_new(
                ConnectorInstanceDescriptor {
                    provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                        .expect("fixture provider ID"),
                    instance_id,
                },
                None,
                read,
            )
            .expect("fixture connector instance"),
        )
        .expect("register planned-files fixture");
}
