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

use std::collections::VecDeque;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
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
use super::scan_range::plan_iceberg_file_ranges;
use crate::cache::{CacheOptions, DataCacheContext};
use crate::common::ids::SlotId;
use crate::connector::HdfsScanConfig;
use crate::connector::hdfs::HdfsScanOp;
use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
use crate::exec::node::BoxedExecIter;
use crate::exec::node::scan::ScanOp;
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
        };
        let loaded =
            load_table(&entry, &table.namespace, &table.table).map_err(map_iceberg_error)?;
        let schema = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(loaded.table.metadata().current_schema())
                .map_err(|error| internal(format!("convert Iceberg schema to Arrow: {error}")))?,
        );
        let version = loaded
            .table
            .metadata()
            .current_snapshot_id()
            .map(|snapshot_id| Bytes::from(snapshot_id.to_le_bytes().to_vec()));
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
        let snapshot_id = self.select_snapshot(&entry, &table, request.selector)?;
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
        let Some(snapshot_id) = scan.snapshot_id else {
            return Ok(Vec::new());
        };
        let entry = self.entry(self.instance_id.as_str())?;
        let loaded = load_table(&entry, &scan.table.namespace, &scan.table.table)
            .map_err(map_iceberg_error)?;
        let files = extract_data_files_with_stats_at(&loaded.table, snapshot_id)
            .map_err(map_iceberg_error)?;
        let mut remaining = scan.limit;
        let splits = files
            .into_iter()
            .enumerate()
            .map(|(index, file)| {
                if let Some(remaining_rows) = remaining.as_mut() {
                    if *remaining_rows == 0 {
                        return Ok(None);
                    }
                    if let Some(row_count) = file
                        .record_count
                        .and_then(|count| u64::try_from(count).ok())
                    {
                        *remaining_rows = remaining_rows.saturating_sub(row_count);
                    }
                }
                let estimated_bytes = u64::try_from(file.size).ok();
                let payload = SplitPayload {
                    table: scan.table.clone(),
                    snapshot_id,
                    path: file.path,
                    projection: scan.projection.clone(),
                    limit: scan.limit,
                };
                Ok(Some(ConnectorSplit::try_new(
                    self.instance_id.clone(),
                    format!("{snapshot_id}-{index}"),
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
        let file = extract_data_files_with_stats_at(&loaded.table, split.snapshot_id)
            .map_err(map_iceberg_error)?
            .into_iter()
            .find(|file| file.path == split.path)
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::NotFound,
                    "Iceberg split data file is absent from its pinned snapshot",
                )
            })?;
        let ranges = plan_iceberg_file_ranges(
            &super::catalog::backend::data_file_with_stats_to_iceberg_data_file_info(file),
        )
        .map_err(map_iceberg_error)?;
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
        let op = Arc::new(HdfsScanOp::new(HdfsScanConfig {
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
        }));
        let morsels = op.build_morsels().map_err(map_iceberg_error)?.morsels;
        Ok(Box::new(IcebergBatchReader {
            op,
            morsels: VecDeque::from(morsels),
            current: None,
            context: request.context,
            max_rows: request.batch.max_rows.get(),
            max_bytes: request.batch.max_bytes.get(),
            closed: false,
        }))
    }
}

#[derive(Clone, Deserialize, Serialize)]
struct TablePayload {
    namespace: String,
    table: String,
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
    snapshot_id: i64,
    path: String,
    projection: Vec<usize>,
    limit: Option<u64>,
}

struct IcebergBatchReader {
    op: Arc<HdfsScanOp>,
    morsels: VecDeque<crate::exec::node::scan::ScanMorsel>,
    current: Option<BoxedExecIter>,
    context: novarocks_spi::connector::ConnectorRequestContext,
    max_rows: usize,
    max_bytes: usize,
    closed: bool,
}

impl IcebergBatchReader {
    fn validate_context(&self) -> Result<(), ConnectorError> {
        if self.context.cancellation().is_cancelled() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Cancelled,
                "connector request was cancelled",
            ));
        }
        if Instant::now() >= self.context.deadline() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::DeadlineExceeded,
                "connector request deadline elapsed",
            ));
        }
        Ok(())
    }
}

impl ConnectorBatchReader for IcebergBatchReader {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        self.validate_context()?;
        if self.closed {
            return Ok(None);
        }
        loop {
            if let Some(current) = self.current.as_mut() {
                match current.next() {
                    Some(Ok(chunk)) => {
                        let batch = chunk.batch;
                        if batch.num_rows() > self.max_rows
                            || batch.get_array_memory_size() > self.max_bytes
                        {
                            return Err(ConnectorError::new(
                                ConnectorErrorKind::ResourceExhausted,
                                "Iceberg reader exceeded its batch budget",
                            ));
                        }
                        return Ok(Some(batch));
                    }
                    Some(Err(error)) => return Err(map_iceberg_error(error)),
                    None => self.current = None,
                }
            }
            let Some(morsel) = self.morsels.pop_front() else {
                self.closed = true;
                return Ok(None);
            };
            self.current = Some(
                self.op
                    .execute_iter(morsel, None, None)
                    .map_err(map_iceberg_error)?,
            );
        }
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        self.current = None;
        self.morsels.clear();
        self.closed = true;
        Ok(())
    }
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
