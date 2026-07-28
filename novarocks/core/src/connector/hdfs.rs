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
use std::collections::{HashMap, VecDeque};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, Int32Array, Int64Array,
    TimestampMicrosecondArray, new_null_array,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use novarocks_spi::connector::{
    ConnectorBatchBudget, ConnectorBatchReader, ConnectorBeginScanRequest, ConnectorCancellation,
    ConnectorError, ConnectorErrorKind, ConnectorInstance, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorOpenReaderRequest, ConnectorProviderId, ConnectorRead,
    ConnectorRequestContext, ConnectorScan, ConnectorScanHandle, ConnectorSplit,
    ConnectorSplitPlanningRequest, ConnectorTableHandle, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
    MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
};

use crate::cache::{CacheOptions, DataCacheContext};
use crate::common::ids::SlotId;
use crate::common::runtime_scan_predicate::RuntimeScanPredicateCounters;
use crate::connector::ConnectorRegistry;
use crate::connector::host::ConnectorTransportFactory;
use crate::connector::iceberg::delete_file::{IcebergDeleteFileSpec, IcebergFileContent};
use crate::connector::iceberg::file_pruning::{IcebergFileNullState, IcebergFilePruningCounters};
use crate::connector::iceberg::position_delete::load_position_deletes;
use crate::connector::runtime::{
    ConnectorReadAuxiliary, ConnectorReadCoreFacet, ConnectorReadScanSource,
    ConnectorScheduledSplit, ConnectorSplitAppend, IncrementalConnectorSplitAdapter,
};
use crate::exec::node::BoxedExecIter;
use crate::exec::node::scan::{
    HdfsScanFileFormat, IncrementalScanRange, RuntimeFilterContext, ScanMorsel,
    ScanMorselPruneDecision, ScanMorsels, ScanSource,
};
use crate::formats::parquet::{ParquetReadCachePolicy, ParquetScanConfig, ParquetSlotKind};
use crate::formats::{FileFormatConfig, build_format_iter};
use crate::fs::scan_context::{FileScanContext, FileScanRange};
use crate::runtime::profile::RuntimeProfile;
use crate::runtime::query_context::{QueryId, query_context_manager};
use crate::runtime::query_options::{QueryOptions, query_expire_durations};
use crate::runtime_filter::exec::ordered_range_predicate::NativeOrderedRangePredicate;

const HDFS_SPI_PROVIDER_ID: &str = "hdfs";

/// BE-local native transport factory for file-backed connector reads. The
/// provider reads object-store access from startup config; transport payloads
/// are limited to provider-owned scan state and core file sidecars.
pub(crate) struct HdfsNativeTransportFactory {
    provider_id: ConnectorProviderId,
}

impl HdfsNativeTransportFactory {
    pub(crate) fn new() -> Self {
        Self {
            provider_id: ConnectorProviderId::parse(HDFS_SPI_PROVIDER_ID)
                .expect("static HDFS provider ID is valid"),
        }
    }
}

impl ConnectorTransportFactory for HdfsNativeTransportFactory {
    fn provider_id(&self) -> &ConnectorProviderId {
        &self.provider_id
    }

    fn materialize(
        &self,
        instance_id: ConnectorInstanceId,
        scan_payload: bytes::Bytes,
        file_ranges: &[FileScanRange],
        output_schema: crate::exec::chunk::ChunkSchemaRef,
    ) -> Result<ConnectorInstance, ConnectorError> {
        if !scan_payload.is_empty() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "HDFS native transport scan payload must be empty",
            ));
        }
        let cache_options = CacheOptions::from_query_options(None).map_err(|error| {
            ConnectorError::new(ConnectorErrorKind::Internal, error.to_string())
        })?;
        let object_store_config = crate::common::app_config::config()
            .map_err(|error| ConnectorError::new(ConnectorErrorKind::Internal, error.to_string()))?
            .connector
            .object_store_config()
            .map_err(|error| ConnectorError::new(ConnectorErrorKind::InvalidRequest, error))?;
        let parquet = ParquetScanConfig {
            columns: output_schema
                .slots()
                .iter()
                .map(|slot| slot.name().to_string())
                .collect(),
            chunk_schema: output_schema.clone(),
            slot_kinds: output_schema
                .slots()
                .iter()
                .map(|_| ParquetSlotKind::Regular)
                .collect(),
            case_sensitive: true,
            enable_page_index: false,
            min_max_predicates: Vec::new(),
            runtime_min_max_filter_columns: HashMap::new(),
            variant_path_predicates: Vec::new(),
            batch_size: None,
            datacache: DataCacheContext::external(cache_options),
            cache_policy: ParquetReadCachePolicy::with_flags(false, false, None),
            profile_label: Some("native_connector_hdfs".to_string()),
            iceberg_output_schema: Some(output_schema.arrow_schema_ref()),
            variant_path_columns: Vec::new(),
            query_global_dicts: Default::default(),
        };
        Arc::new(HdfsConnectorInstance::new(
            instance_id,
            HdfsInstanceConfig {
                scan: HdfsScanConfig {
                    original_range_count: file_ranges.len(),
                    ranges: file_ranges.to_vec(),
                    has_more: false,
                    limit: None,
                    profile_label: Some("native_connector_hdfs".to_string()),
                    format: Some(FileFormatConfig::Parquet(parquet)),
                    object_store_config,
                    iceberg_table_locations: HashMap::new(),
                    query_global_dicts: Default::default(),
                    iceberg_runtime_pruning: None,
                },
                chunk_schema: output_schema,
            },
        ))
        .connector_instance()
    }
}

#[derive(Clone)]
pub(crate) struct HdfsInstanceConfig {
    pub(crate) scan: HdfsScanConfig,
    pub(crate) chunk_schema: crate::exec::chunk::ChunkSchemaRef,
}

pub(crate) struct HdfsConnectorInstance {
    instance_id: ConnectorInstanceId,
    config: HdfsInstanceConfig,
    ranges: Mutex<Vec<FileScanRange>>,
    next_scan_range_id: AtomicI32,
    incremental_lock: Mutex<()>,
}

impl HdfsConnectorInstance {
    pub(crate) fn new(instance_id: ConnectorInstanceId, config: HdfsInstanceConfig) -> Self {
        let next_scan_range_id = config
            .scan
            .ranges
            .iter()
            .filter_map(|range| (range.scan_range_id >= 0).then_some(range.scan_range_id))
            .max()
            .map(|value| value.saturating_add(1))
            .unwrap_or(0);
        let ranges = config.scan.ranges.clone();
        Self {
            instance_id,
            config,
            ranges: Mutex::new(ranges),
            next_scan_range_id: AtomicI32::new(next_scan_range_id),
            incremental_lock: Mutex::new(()),
        }
    }

    pub(crate) fn connector_instance(self: Arc<Self>) -> Result<ConnectorInstance, ConnectorError> {
        ConnectorInstance::try_new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse(HDFS_SPI_PROVIDER_ID)?,
                instance_id: self.instance_id.clone(),
            },
            None,
            self,
        )
    }

    fn validate_context(&self, context: &ConnectorRequestContext) -> Result<(), ConnectorError> {
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

    fn split_for_index(&self, index: usize) -> Result<ConnectorSplit, ConnectorError> {
        let ranges = self.ranges.lock().map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "HDFS range state lock poisoned",
            )
        })?;
        let range = ranges.get(index).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "HDFS split index is out of bounds",
            )
        })?;
        ConnectorSplit::try_new(
            self.instance_id.clone(),
            format!("hdfs-{index}"),
            bytes::Bytes::copy_from_slice(&(index as u64).to_le_bytes()),
            Some(range.length),
        )
    }

    fn range_for_index(&self, index: usize) -> Result<FileScanRange, ConnectorError> {
        self.ranges
            .lock()
            .map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "HDFS range state lock poisoned",
                )
            })?
            .get(index)
            .cloned()
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "HDFS split index is out of bounds",
                )
            })
    }

    fn range_count(&self) -> Result<usize, ConnectorError> {
        self.ranges.lock().map(|ranges| ranges.len()).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "HDFS range state lock poisoned",
            )
        })
    }

    fn split_index(&self, split: &ConnectorSplit) -> Result<usize, ConnectorError> {
        if split.owner() != &self.instance_id || split.payload().len() != 8 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "invalid HDFS split payload",
            ));
        }
        let bytes: [u8; 8] = split
            .payload()
            .as_ref()
            .try_into()
            .expect("payload length checked");
        usize::try_from(u64::from_le_bytes(bytes)).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "HDFS split index overflows usize",
            )
        })
    }

    fn prepare_incremental_ranges(
        &self,
        scan_ranges: &[IncrementalScanRange],
    ) -> Result<ConnectorSplitAppend, String> {
        let _append = self
            .incremental_lock
            .lock()
            .map_err(|_| "HDFS incremental range lock poisoned".to_string())?;
        let ranges = self
            .ranges
            .lock()
            .map_err(|_| "HDFS range state lock poisoned".to_string())?;
        let row_position_scan = ranges
            .iter()
            .any(|range| range.scan_range_id >= 0 || range.first_row_id.is_some());
        let expected_file_format = match self.config.scan.format.as_ref() {
            Some(FileFormatConfig::Parquet(_)) => Some(HdfsScanFileFormat::Parquet),
            Some(FileFormatConfig::Orc(_)) => Some(HdfsScanFileFormat::Orc),
            None => None,
        };
        let mut next_scan_range_id = self.next_scan_range_id.load(Ordering::Acquire);
        let mut has_more = false;
        let mut appended = Vec::new();

        for scan_range in scan_ranges {
            if let Some(value) = scan_range.has_more() {
                has_more = value;
            }
            let IncrementalScanRange::Hdfs {
                range: hdfs_range, ..
            } = scan_range
            else {
                continue;
            };
            if let Some(expected) = expected_file_format {
                let file_format = hdfs_range.file_format.ok_or_else(|| {
                    "incremental hdfs scan range is missing file_format".to_string()
                })?;
                if file_format != expected {
                    return Err(format!(
                        "incremental hdfs scan range file_format mismatch: expected {:?}, got {:?}",
                        expected, file_format
                    ));
                }
            }
            let path = if let Some(path) = hdfs_range
                .full_path
                .as_ref()
                .map(|path| path.trim())
                .filter(|path| !path.is_empty())
            {
                path.to_string()
            } else if let Some(relative_path) = hdfs_range
                .relative_path
                .as_ref()
                .map(|path| path.trim())
                .filter(|path| !path.is_empty())
            {
                let table_id = hdfs_range.table_id.ok_or_else(|| {
                    "incremental hdfs scan range has relative_path but missing table_id".to_string()
                })?;
                let base = self
                    .config
                    .scan
                    .iceberg_table_locations
                    .get(&table_id)
                    .map(|location| location.trim_end_matches('/'))
                    .ok_or_else(|| {
                        format!(
                            "incremental hdfs scan range missing cached iceberg location for table_id={table_id}"
                        )
                    })?;
                let relative_path = relative_path.trim_start_matches('/');
                if relative_path.is_empty() {
                    base.to_string()
                } else {
                    format!("{base}/{relative_path}")
                }
            } else {
                return Err(
                    "incremental hdfs scan range requires non-empty full_path or relative_path"
                        .to_string(),
                );
            };
            let file_len = u64::try_from(hdfs_range.file_length).unwrap_or(0);
            let offset = u64::try_from(hdfs_range.offset).unwrap_or(0);
            let mut length = u64::try_from(hdfs_range.length).unwrap_or(0);
            if length == 0 && file_len > offset {
                length = file_len - offset;
            }
            let (scan_range_id, first_row_id) = if row_position_scan {
                let first_row_id = hdfs_range.first_row_id.ok_or_else(|| {
                    "incremental hdfs scan range missing first_row_id for row position scan"
                        .to_string()
                })?;
                let scan_range_id = next_scan_range_id;
                next_scan_range_id = next_scan_range_id.saturating_add(1);
                (scan_range_id, Some(first_row_id))
            } else {
                (-1, None)
            };
            let delete_files = if let Some(range) = ranges.iter().find(|range| {
                range.path == path && range.offset == offset && range.length == length
            }) {
                range.delete_files.clone()
            } else {
                let same_path_delete_file_count = ranges
                    .iter()
                    .filter(|range| range.path == path && !range.delete_files.is_empty())
                    .count();
                if same_path_delete_file_count > 0 {
                    return Err(format!(
                        "incremental HDFS range cannot safely reuse lowered Iceberg delete files for \
                         path={path} offset={offset} length={length}; found \
                         {same_path_delete_file_count} same-path lowered range(s) with delete files but \
                         no exact match"
                    ));
                }
                Vec::new()
            };
            appended.push(FileScanRange {
                path,
                file_len,
                offset,
                length,
                scan_range_id,
                first_row_id,
                data_sequence_number: None,
                ivm_change_op: hdfs_range.ivm_change_op,
                included_positions: None,
                external_datacache: hdfs_range.external_datacache.clone(),
                delete_files,
                iceberg_file_pruning: None,
            });
        }

        let start = ranges.len();
        let scheduled = appended
            .into_iter()
            .enumerate()
            .map(|(offset, range)| {
                let index = start + offset;
                ConnectorSplit::try_new(
                    self.instance_id.clone(),
                    format!("hdfs-{index}"),
                    bytes::Bytes::copy_from_slice(&(index as u64).to_le_bytes()),
                    Some(range.length),
                )
                .map(|split| ConnectorScheduledSplit::file(split, range))
                .map_err(|error| error.to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ConnectorSplitAppend::Scheduled {
            scheduled,
            has_more,
        })
    }

    fn commit_incremental_ranges(&self, append: &ConnectorSplitAppend) -> Result<(), String> {
        let scheduled = append.scheduled_file_splits().ok_or_else(|| {
            "HDFS incremental append must retain scheduled file split sidecars".to_string()
        })?;
        let _append = self
            .incremental_lock
            .lock()
            .map_err(|_| "HDFS incremental range lock poisoned".to_string())?;
        let mut ranges = self
            .ranges
            .lock()
            .map_err(|_| "HDFS range state lock poisoned".to_string())?;
        let start = ranges.len();
        let mut next_scan_range_id = self.next_scan_range_id.load(Ordering::Acquire);
        let mut prepared_ranges = Vec::with_capacity(scheduled.len());

        for (offset, scheduled) in scheduled.iter().enumerate() {
            let index = start + offset;
            let expected_payload = bytes::Bytes::copy_from_slice(&(index as u64).to_le_bytes());
            let split = scheduled.split();
            if split.owner() != &self.instance_id
                || split.split_id() != format!("hdfs-{index}")
                || split.payload() != &expected_payload
            {
                return Err(
                    "HDFS incremental append no longer matches provider range state".to_string(),
                );
            }
            let range = scheduled.file_range().ok_or_else(|| {
                "HDFS incremental append is missing its file range sidecar".to_string()
            })?;
            if range.scan_range_id >= 0 {
                let next = range
                    .scan_range_id
                    .checked_add(1)
                    .ok_or_else(|| "HDFS scan range ID overflowed".to_string())?;
                next_scan_range_id = next_scan_range_id.max(next);
            }
            prepared_ranges.push(range.clone());
        }

        ranges.extend(prepared_ranges);
        self.next_scan_range_id
            .store(next_scan_range_id, Ordering::Release);
        Ok(())
    }
}

struct HdfsIncrementalSplitAdapter {
    provider: Arc<HdfsConnectorInstance>,
}

impl IncrementalConnectorSplitAdapter for HdfsIncrementalSplitAdapter {
    fn prepare_incremental_ranges(
        &self,
        ranges: &[IncrementalScanRange],
    ) -> Result<ConnectorSplitAppend, String> {
        self.provider.prepare_incremental_ranges(ranges)
    }

    fn commit_incremental_ranges(&self, append: &ConnectorSplitAppend) -> Result<(), String> {
        self.provider.commit_incremental_ranges(append)
    }
}

pub(crate) fn plan_hdfs_read_source(
    connectors: &ConnectorRegistry,
    instance_id: ConnectorInstanceId,
    config: HdfsInstanceConfig,
    batch: ConnectorBatchBudget,
    context: ConnectorRequestContext,
) -> Result<Arc<dyn ScanSource>, ConnectorError> {
    let provider = Arc::new(HdfsConnectorInstance::new(instance_id, config));
    let auxiliary = Arc::new(HdfsDeleteAuxiliary::new(
        provider.config.scan.object_store_config.clone(),
    ));
    let scheduled = (0..provider.range_count()?)
        .map(|index| {
            Ok(ConnectorScheduledSplit::file(
                provider.split_for_index(index)?,
                provider.range_for_index(index)?,
            ))
        })
        .collect::<Result<Vec<_>, ConnectorError>>()?;
    let expected_schema = provider.config.chunk_schema.arrow_schema_ref();
    let chunk_schema = Arc::clone(&provider.config.chunk_schema);
    let has_more = provider.config.scan.has_more;
    let incremental = has_more.then(|| {
        Arc::new(HdfsIncrementalSplitAdapter {
            provider: Arc::clone(&provider),
        }) as Arc<dyn IncrementalConnectorSplitAdapter>
    });
    let instance = Arc::clone(&provider).connector_instance()?;
    let (instance, lifecycle) = connectors
        .register_ephemeral_connector_instance(instance)
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::Unavailable, error.to_string()))?;
    let facet = Arc::new(HdfsRuntimePruningFacet::new(provider.config.scan.clone()));
    Ok(Arc::new(
        ConnectorReadScanSource::new_scheduled_ephemeral_with_incremental(
            instance,
            scheduled,
            ConnectorOpenReaderRequest {
                expected_schema,
                batch,
                context,
            },
            chunk_schema,
            lifecycle,
            incremental,
            has_more,
            Some(auxiliary),
        )
        .with_core_facet(facet),
    ))
}

struct HdfsQueryCancellation {
    query_id: QueryId,
}

impl ConnectorCancellation for HdfsQueryCancellation {
    fn is_cancelled(&self) -> bool {
        query_context_manager().is_query_canceled(self.query_id)
    }
}

pub(crate) fn plan_starrocks_hdfs_read_source(
    connectors: &ConnectorRegistry,
    query_id: QueryId,
    node_id: i32,
    config: HdfsInstanceConfig,
    query_options: &QueryOptions,
) -> Result<Arc<dyn ScanSource>, ConnectorError> {
    let rows = query_options
        .batch_size
        .and_then(|value| usize::try_from(value).ok())
        .and_then(NonZeroUsize::new)
        .unwrap_or_else(|| NonZeroUsize::new(4096).expect("default batch size is nonzero"));
    let batch = ConnectorBatchBudget {
        max_rows: rows,
        max_bytes: NonZeroUsize::new(MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES)
            .expect("SPI handle maximum is nonzero"),
    };
    let (_, query_expire) = query_expire_durations(Some(query_options));
    let context = ConnectorRequestContext::try_new(
        Instant::now() + query_expire,
        Arc::new(HdfsQueryCancellation { query_id }),
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )?;
    plan_hdfs_read_source(
        connectors,
        ConnectorInstanceId::parse(&format!("hdfs.{query_id}.{node_id}"))?,
        config,
        batch,
        context,
    )
}

struct NativeHdfsCancellation {
    query_id: Option<QueryId>,
}

impl ConnectorCancellation for NativeHdfsCancellation {
    fn is_cancelled(&self) -> bool {
        self.query_id
            .is_some_and(|query_id| query_context_manager().is_query_canceled(query_id))
    }
}

pub(crate) fn plan_native_hdfs_read_source(
    connectors: &ConnectorRegistry,
    query_id: Option<QueryId>,
    node_id: i32,
    config: HdfsInstanceConfig,
    query_options: &QueryOptions,
) -> Result<Arc<dyn ScanSource>, ConnectorError> {
    let rows = query_options
        .batch_size
        .and_then(|value| usize::try_from(value).ok())
        .and_then(NonZeroUsize::new)
        .unwrap_or_else(|| NonZeroUsize::new(4096).expect("default batch size is nonzero"));
    let batch = ConnectorBatchBudget {
        max_rows: rows,
        max_bytes: NonZeroUsize::new(MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES)
            .expect("SPI handle maximum is nonzero"),
    };
    let (_, query_expire) = query_expire_durations(Some(query_options));
    let context = ConnectorRequestContext::try_new(
        Instant::now() + query_expire,
        Arc::new(NativeHdfsCancellation { query_id }),
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )?;
    let instance_query_id = query_id
        .map(|query_id| query_id.to_string())
        .unwrap_or_else(|| "unidentified".to_string());
    plan_hdfs_read_source(
        connectors,
        ConnectorInstanceId::parse(&format!("hdfs.native.{instance_query_id}.{node_id}"))?,
        config,
        batch,
        context,
    )
}

impl ConnectorRead for HdfsConnectorInstance {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }

    fn begin_scan(
        &self,
        table: &ConnectorTableHandle,
        request: ConnectorBeginScanRequest,
    ) -> Result<ConnectorScan, ConnectorError> {
        self.validate_context(&request.context)?;
        if table.owner() != &self.instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "HDFS table handle belongs to another instance",
            ));
        }
        Ok(ConnectorScan {
            handle: ConnectorScanHandle::try_new(self.instance_id.clone(), bytes::Bytes::new())?,
            output_schema: self.config.chunk_schema.arrow_schema_ref(),
        })
    }

    fn plan_splits(
        &self,
        scan: &ConnectorScanHandle,
        request: ConnectorSplitPlanningRequest,
    ) -> Result<Vec<ConnectorSplit>, ConnectorError> {
        self.validate_context(&request.context)?;
        if scan.owner() != &self.instance_id || !scan.payload().is_empty() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "invalid HDFS scan handle",
            ));
        }
        (0..self.range_count()?)
            .map(|index| self.split_for_index(index))
            .collect()
    }

    fn open_reader(
        &self,
        split: &ConnectorSplit,
        request: ConnectorOpenReaderRequest,
    ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
        self.validate_context(&request.context)?;
        if request.expected_schema.as_ref() != self.config.chunk_schema.arrow_schema_ref().as_ref()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "HDFS reader expected schema does not match decoded output schema",
            ));
        }
        let index = self.split_index(split)?;
        let range = self.range_for_index(index)?;
        let mut scan = self.config.scan.clone();
        scan.original_range_count = 1;
        scan.ranges = vec![range];
        Ok(Box::new(HdfsFileBatchReader::new(
            scan,
            request.context,
            request.batch.max_rows.get(),
            request.batch.max_bytes.get(),
        )))
    }
}

fn delete_files_have_position_deletes(delete_files: &[IcebergDeleteFileSpec]) -> bool {
    delete_files
        .iter()
        .any(|file| file.file_content == IcebergFileContent::PositionDeletes)
}

fn apply_parquet_pruning_gate_for_delete_files(
    parquet_cfg: &mut crate::formats::parquet::ParquetScanConfig,
    delete_files: &[IcebergDeleteFileSpec],
) {
    if delete_files_have_position_deletes(delete_files) {
        parquet_cfg.enable_page_index = false;
        parquet_cfg.min_max_predicates.clear();
        parquet_cfg.runtime_min_max_filter_columns.clear();
        parquet_cfg.variant_path_predicates.clear();
    }
}

/// Opens one provider-owned file range without depending on the core `ScanOp`
/// contract. Both the transitional HDFS scan op and the SPI batch reader use
/// this seam so position-delete pruning behavior stays identical.
pub(crate) fn build_hdfs_range_iter(
    cfg: &HdfsScanConfig,
    range: FileScanRange,
    profile: Option<RuntimeProfile>,
    runtime_filters: Option<&RuntimeFilterContext>,
) -> Result<BoxedExecIter, String> {
    let external_datacache = range.external_datacache.clone();
    let scan = FileScanContext::build(
        vec![range],
        profile.clone(),
        cfg.object_store_config.as_ref(),
    )?;
    if let Some(profile) = profile.as_ref() {
        profile.add_info_string(
            "OriginalRangeCount",
            format!("{}", cfg.original_range_count),
        );
        profile.add_info_string("RangeCount", format!("{}", scan.ranges.len()));
    }
    let current_delete_files = scan
        .ranges
        .first()
        .map(|range| range.delete_files.as_slice())
        .unwrap_or(&[]);
    let Some(mut format) = cfg.format.clone() else {
        return Err("hdfs scan missing file format for non-empty morsel".to_string());
    };
    format = match format {
        FileFormatConfig::Parquet(mut parquet_cfg) => {
            parquet_cfg.datacache = parquet_cfg
                .datacache
                .with_external_range_options(external_datacache.as_ref())?;
            parquet_cfg.query_global_dicts = cfg.query_global_dicts.clone();
            apply_parquet_pruning_gate_for_delete_files(&mut parquet_cfg, current_delete_files);
            FileFormatConfig::Parquet(parquet_cfg)
        }
        FileFormatConfig::Orc(mut orc_cfg) => {
            orc_cfg.datacache = orc_cfg
                .datacache
                .with_external_range_options(external_datacache.as_ref())?;
            FileFormatConfig::Orc(orc_cfg)
        }
    };
    build_format_iter(scan, format, None, profile, runtime_filters)
}

fn exact_ordered_file_candidates(
    stats: &crate::connector::iceberg::scan_model::IcebergColumnStats,
    explicit_null_state: Option<IcebergFileNullState>,
    data_type: &DataType,
) -> Option<ArrayRef> {
    let has_null = match (stats.value_count, stats.null_count) {
        (Some(value_count), Some(null_count)) => {
            if value_count < 0 || null_count < 0 || null_count > value_count {
                return None;
            }
            if value_count == 0 {
                return Some(new_null_array(data_type, 0));
            }
            if value_count == null_count {
                return Some(new_null_array(data_type, 1));
            }
            null_count > 0
        }
        (None, None) => match explicit_null_state? {
            IcebergFileNullState::NoNulls => false,
            IcebergFileNullState::HasNulls => true,
            IcebergFileNullState::AllNull => return Some(new_null_array(data_type, 1)),
        },
        (Some(_), None) | (None, Some(_)) => return None,
    };
    let lower = stats.lower_bound.as_deref()?;
    let upper = stats.upper_bound.as_deref()?;
    macro_rules! candidates {
        ($lower:expr, $upper:expr, $array:ident $(, $finish:expr)?) => {{
            let lower = $lower;
            let upper = $upper;
            if lower > upper {
                return None;
            }
            let values = if has_null {
                vec![Some(lower), Some(upper), None]
            } else {
                vec![Some(lower), Some(upper)]
            };
            let array = $array::from(values);
            $(
                let array = $finish(array)?;
            )?
            Some(Arc::new(array) as ArrayRef)
        }};
    }
    match data_type {
        DataType::Boolean => {
            let decode = |bytes: &[u8]| match bytes {
                [0] => Some(false),
                [1] => Some(true),
                _ => None,
            };
            candidates!(decode(lower)?, decode(upper)?, BooleanArray)
        }
        DataType::Int32 => {
            candidates!(
                i32::from_le_bytes(lower.try_into().ok()?),
                i32::from_le_bytes(upper.try_into().ok()?),
                Int32Array
            )
        }
        DataType::Int64 => {
            candidates!(
                i64::from_le_bytes(lower.try_into().ok()?),
                i64::from_le_bytes(upper.try_into().ok()?),
                Int64Array
            )
        }
        DataType::Date32 => {
            candidates!(
                i32::from_le_bytes(lower.try_into().ok()?),
                i32::from_le_bytes(upper.try_into().ok()?),
                Date32Array
            )
        }
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
            let lower = i64::from_le_bytes(lower.try_into().ok()?);
            let upper = i64::from_le_bytes(upper.try_into().ok()?);
            if lower > upper {
                return None;
            }
            let values = if has_null {
                vec![Some(lower), Some(upper), None]
            } else {
                vec![Some(lower), Some(upper)]
            };
            Some(Arc::new(
                TimestampMicrosecondArray::from(values).with_timezone_opt(timezone.clone()),
            ) as ArrayRef)
        }
        DataType::Decimal128(precision, scale) => {
            let decode = |bytes: &[u8]| {
                if bytes.is_empty() || bytes.len() > 16 {
                    return None;
                }
                let fill = if bytes[0] & 0x80 == 0 { 0 } else { u8::MAX };
                let mut decoded = [fill; 16];
                decoded[16 - bytes.len()..].copy_from_slice(bytes);
                Some(i128::from_be_bytes(decoded))
            };
            candidates!(
                decode(lower)?,
                decode(upper)?,
                Decimal128Array,
                |array: Decimal128Array| array.with_precision_and_scale(*precision, *scale).ok()
            )
        }
        _ => None,
    }
}

#[derive(Clone, Debug, Default)]
pub struct HdfsIcebergRuntimePruningConfig {
    pub slot_to_column: HashMap<SlotId, String>,
    pub min_max_filter_columns: HashMap<i32, String>,
    pub discrete_set_max_values: usize,
}

#[derive(Clone, Debug)]
pub struct HdfsScanConfig {
    pub ranges: Vec<FileScanRange>,
    /// Original range count from FE `per_node_scan_ranges` before any local coalescing.
    /// This is useful for profiling/debugging when multiple splits point to the same file.
    pub original_range_count: usize,
    pub has_more: bool,
    pub limit: Option<usize>,
    pub profile_label: Option<String>,
    pub format: Option<FileFormatConfig>,
    /// OSS credentials supplied by FE via `THdfsScanNode.cloud_configuration`.
    /// Used as a fallback when the shard registry has no entry for the scanned path
    /// (typical for Iceberg external tables whose files are not tracked as lake tablets).
    pub object_store_config: Option<crate::fs::object_store::ObjectStoreConfig>,
    /// Cached Iceberg table locations keyed by `table_id`, used to resolve incremental
    /// scan ranges that only carry `relative_path`.
    pub iceberg_table_locations: HashMap<i64, String>,
    /// Per-slot global dictionary encode maps (string bytes -> dict id) for
    /// dict-encoded output columns. Empty for all non-dict scans. Injected into
    /// the parquet format config in `execute_iter`; the reader reads the dict
    /// column as Utf8 and encodes the strings to ids.
    pub query_global_dicts: crate::exec::dict_encode::QueryGlobalDictEncodeMap,
    pub iceberg_runtime_pruning: Option<HdfsIcebergRuntimePruningConfig>,
}

/// Provider-owned pull reader for file ranges. It deliberately has no core
/// `ScanOp` dependency and returns Arrow batches to the SPI boundary.
pub(crate) struct HdfsFileBatchReader {
    cfg: HdfsScanConfig,
    ranges: VecDeque<FileScanRange>,
    current: Option<BoxedExecIter>,
    context: ConnectorRequestContext,
    max_rows: usize,
    max_bytes: usize,
    closed: bool,
}

impl HdfsFileBatchReader {
    pub(crate) fn new(
        mut cfg: HdfsScanConfig,
        context: ConnectorRequestContext,
        max_rows: usize,
        max_bytes: usize,
    ) -> Self {
        let ranges = std::mem::take(&mut cfg.ranges);
        Self {
            cfg,
            ranges: VecDeque::from(ranges),
            current: None,
            context,
            max_rows,
            max_bytes,
            closed: false,
        }
    }

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

impl ConnectorBatchReader for HdfsFileBatchReader {
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
                                "HDFS reader exceeded its batch budget",
                            ));
                        }
                        return Ok(Some(batch));
                    }
                    Some(Err(error)) => {
                        return Err(ConnectorError::new(ConnectorErrorKind::Internal, error));
                    }
                    None => self.current = None,
                }
            }
            let Some(range) = self.ranges.pop_front() else {
                self.closed = true;
                return Ok(None);
            };
            self.current = Some(
                build_hdfs_range_iter(&self.cfg, range, None, None)
                    .map_err(|error| ConnectorError::new(ConnectorErrorKind::Internal, error))?,
            );
        }
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        self.current = None;
        self.ranges.clear();
        self.closed = true;
        Ok(())
    }
}

/// Provider-private Iceberg delete-file loader. It keeps credential resolution
/// in the HDFS provider while the core scan runner owns delete filtering.
pub(crate) struct HdfsDeleteAuxiliary {
    object_store_config: Option<crate::fs::object_store::ObjectStoreConfig>,
}

impl HdfsDeleteAuxiliary {
    pub(crate) fn new(
        object_store_config: Option<crate::fs::object_store::ObjectStoreConfig>,
    ) -> Self {
        Self {
            object_store_config,
        }
    }

    fn normalized_delete_specs(
        &self,
        range: &FileScanRange,
    ) -> Result<
        (
            String,
            Vec<IcebergDeleteFileSpec>,
            crate::fs::scan_context::FileScanContext,
        ),
        String,
    > {
        let mut loader_ranges = Vec::with_capacity(1 + range.delete_files.len());
        loader_ranges.push(FileScanRange {
            path: range.path.clone(),
            file_len: 0,
            offset: 0,
            length: 0,
            scan_range_id: -1,
            first_row_id: None,
            data_sequence_number: None,
            ivm_change_op: None,
            included_positions: None,
            external_datacache: None,
            delete_files: Vec::new(),
            iceberg_file_pruning: None,
        });
        for delete_file in &range.delete_files {
            loader_ranges.push(FileScanRange {
                path: delete_file.path.clone(),
                file_len: delete_file.length.unwrap_or(0),
                offset: 0,
                length: delete_file.length.unwrap_or(0),
                scan_range_id: -1,
                first_row_id: None,
                data_sequence_number: None,
                ivm_change_op: None,
                included_positions: None,
                external_datacache: None,
                delete_files: Vec::new(),
                iceberg_file_pruning: None,
            });
        }
        let context =
            FileScanContext::build(loader_ranges, None, self.object_store_config.as_ref())?;
        let delete_specs = context
            .ranges
            .iter()
            .skip(1)
            .zip(range.delete_files.iter())
            .map(|(resolved, original)| IcebergDeleteFileSpec {
                path: resolved.path.clone(),
                file_format: original.file_format,
                file_content: original.file_content,
                length: original.length,
                content_offset: original.content_offset,
                content_size_in_bytes: original.content_size_in_bytes,
            })
            .collect();
        Ok((range.path.clone(), delete_specs, context))
    }
}

impl ConnectorReadAuxiliary for HdfsDeleteAuxiliary {
    fn load_iceberg_position_deletes(
        &self,
        range: &FileScanRange,
    ) -> Result<Option<roaring::RoaringTreemap>, String> {
        if range.delete_files.is_empty() {
            return Ok(None);
        }
        let (data_file_path, delete_specs, context) = self.normalized_delete_specs(range)?;
        let deleted = load_position_deletes(&delete_specs, &data_file_path, &context.factory)?;
        Ok((!deleted.is_empty()).then_some(deleted))
    }

    fn load_iceberg_equality_deletes(
        &self,
        range: &FileScanRange,
    ) -> Result<Option<Vec<crate::connector::iceberg::equality_delete::EqualityDeleteSet>>, String>
    {
        if !range
            .delete_files
            .iter()
            .any(|file| file.file_content == IcebergFileContent::EqualityDeletes)
        {
            return Ok(None);
        }
        let (_, delete_specs, context) = self.normalized_delete_specs(range)?;
        let sets = crate::connector::iceberg::equality_delete::load_equality_delete_sets(
            &delete_specs,
            &context.factory,
        )?;
        Ok((!sets.is_empty()).then_some(sets))
    }
}

#[derive(Clone, Debug)]
struct HdfsRuntimePruningFacet {
    cfg: HdfsScanConfig,
    row_position_scan: bool,
    next_scan_range_id: Arc<AtomicI32>,
    iceberg_runtime_pruning_counters: Arc<HdfsIcebergRuntimePruningCounters>,
    iceberg_runtime_pruning_profile_flushed: Arc<AtomicBool>,
}

#[derive(Debug, Default)]
struct HdfsIcebergRuntimePruningCounters {
    files_total: AtomicU64,
    files_selected: AtomicU64,
    files_pruned: AtomicU64,
    predicates: AtomicU64,
    unsupported: AtomicU64,
    unavailable: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct HdfsIcebergRuntimePruningCounterSnapshot {
    pub(crate) files_total: u64,
    pub(crate) files_selected: u64,
    pub(crate) files_pruned: u64,
    pub(crate) predicates: u64,
    pub(crate) unsupported: u64,
    pub(crate) unavailable: u64,
}

fn u128_to_u64_saturating(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn u64_to_i64_saturating(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn atomic_add_saturating(counter: &AtomicU64, delta: u64) {
    let _ = counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
        Some(current.saturating_add(delta))
    });
}

impl HdfsIcebergRuntimePruningCounters {
    fn record_runtime_predicates(
        &self,
        predicates: usize,
        predicate_counters: &RuntimeScanPredicateCounters,
    ) {
        atomic_add_saturating(&self.predicates, predicates as u64);
        atomic_add_saturating(
            &self.unsupported,
            u128_to_u64_saturating(predicate_counters.unsupported),
        );
    }

    fn record_file_counters(&self, file_counters: &IcebergFilePruningCounters) {
        atomic_add_saturating(
            &self.files_total,
            u128_to_u64_saturating(file_counters.files_total),
        );
        atomic_add_saturating(
            &self.files_selected,
            u128_to_u64_saturating(file_counters.files_selected),
        );
        atomic_add_saturating(
            &self.files_pruned,
            u128_to_u64_saturating(file_counters.files_pruned),
        );
        atomic_add_saturating(
            &self.unsupported,
            u128_to_u64_saturating(file_counters.unsupported),
        );
    }

    fn record_missing_metadata(&self, ranges: usize) {
        let ranges = ranges as u64;
        atomic_add_saturating(&self.files_total, ranges);
        atomic_add_saturating(&self.files_selected, ranges);
        atomic_add_saturating(&self.unsupported, ranges);
    }

    fn record_unavailable(&self) {
        atomic_add_saturating(&self.unavailable, 1);
    }

    fn snapshot(&self) -> HdfsIcebergRuntimePruningCounterSnapshot {
        HdfsIcebergRuntimePruningCounterSnapshot {
            files_total: self.files_total.load(Ordering::Acquire),
            files_selected: self.files_selected.load(Ordering::Acquire),
            files_pruned: self.files_pruned.load(Ordering::Acquire),
            predicates: self.predicates.load(Ordering::Acquire),
            unsupported: self.unsupported.load(Ordering::Acquire),
            unavailable: self.unavailable.load(Ordering::Acquire),
        }
    }
}

impl HdfsRuntimePruningFacet {
    fn new(cfg: HdfsScanConfig) -> Self {
        let row_position_scan = cfg
            .ranges
            .iter()
            .any(|r| r.scan_range_id >= 0 || r.first_row_id.is_some());
        let next_scan_range_id = cfg
            .ranges
            .iter()
            .filter_map(|r| (r.scan_range_id >= 0).then_some(r.scan_range_id))
            .max()
            .map(|v| v.saturating_add(1))
            .unwrap_or(0);
        Self {
            cfg,
            row_position_scan,
            next_scan_range_id: Arc::new(AtomicI32::new(next_scan_range_id)),
            iceberg_runtime_pruning_counters: Arc::new(HdfsIcebergRuntimePruningCounters::default()),
            iceberg_runtime_pruning_profile_flushed: Arc::new(AtomicBool::new(false)),
        }
    }

    fn expected_hdfs_file_format(&self) -> Option<HdfsScanFileFormat> {
        match self.cfg.format.as_ref() {
            Some(FileFormatConfig::Parquet(_)) => Some(HdfsScanFileFormat::Parquet),
            Some(FileFormatConfig::Orc(_)) => Some(HdfsScanFileFormat::Orc),
            None => None,
        }
    }

    fn next_incremental_scan_range_id(&self) -> i32 {
        self.next_scan_range_id.fetch_add(1, Ordering::AcqRel)
    }

    fn lowered_delete_files_for_range(
        &self,
        path: &str,
        offset: u64,
        length: u64,
    ) -> Result<Vec<IcebergDeleteFileSpec>, String> {
        if let Some(range) =
            self.cfg.ranges.iter().find(|range| {
                range.path == path && range.offset == offset && range.length == length
            })
        {
            return Ok(range.delete_files.clone());
        }

        let same_path_delete_file_count = self
            .cfg
            .ranges
            .iter()
            .filter(|range| range.path == path && !range.delete_files.is_empty())
            .count();
        if same_path_delete_file_count > 0 {
            return Err(format!(
                "incremental HDFS range cannot safely reuse lowered Iceberg delete files for \
                 path={path} offset={offset} length={length}; found \
                 {same_path_delete_file_count} same-path lowered range(s) with delete files but \
                 no exact match"
            ));
        }

        Ok(Vec::new())
    }

    fn ordered_initial_ranges(&self) -> Vec<&FileScanRange> {
        let mut ranges = self.cfg.ranges.iter().collect::<Vec<_>>();
        if self.can_reorder_initial_ranges() {
            ranges.sort_by(|left, right| {
                right
                    .length
                    .cmp(&left.length)
                    .then_with(|| left.path.cmp(&right.path))
                    .then_with(|| left.offset.cmp(&right.offset))
            });
        }
        ranges
    }

    fn can_reorder_initial_ranges(&self) -> bool {
        !self.row_position_scan
            && self.cfg.ranges.iter().all(|range| {
                range.scan_range_id < 0
                    && range.first_row_id.is_none()
                    && range.data_sequence_number.is_none()
                    && range.ivm_change_op.is_none()
                    && range.delete_files.is_empty()
            })
    }

    fn has_iceberg_file_pruning_metadata(&self) -> bool {
        self.cfg
            .ranges
            .iter()
            .any(|range| range.iceberg_file_pruning.is_some())
    }

    fn has_iceberg_runtime_pruning_bindings(pruning_cfg: &HdfsIcebergRuntimePruningConfig) -> bool {
        !pruning_cfg.slot_to_column.is_empty() || !pruning_cfg.min_max_filter_columns.is_empty()
    }

    fn can_materialize_iceberg_runtime_file_pruning(&self) -> bool {
        self.cfg
            .iceberg_runtime_pruning
            .as_ref()
            .is_some_and(Self::has_iceberg_runtime_pruning_bindings)
    }

    fn build_morsels_from_ordered_ranges(
        &self,
        ranges: Vec<&FileScanRange>,
    ) -> Result<ScanMorsels, String> {
        let mut morsels = Vec::with_capacity(ranges.len());
        for r in ranges {
            morsels.push(ScanMorsel::FileRange {
                path: r.path.clone(),
                file_len: r.file_len,
                offset: r.offset,
                length: r.length,
                scan_range_id: r.scan_range_id,
                first_row_id: r.first_row_id,
                data_sequence_number: r.data_sequence_number,
                ivm_change_op: r.ivm_change_op,
                included_positions: r.included_positions.clone(),
                external_datacache: r.external_datacache.clone(),
                delete_files: r.delete_files.clone(),
                iceberg_file_pruning: r.iceberg_file_pruning.clone(),
            });
        }
        Ok(ScanMorsels::new(morsels, self.cfg.has_more))
    }

    fn flush_iceberg_runtime_pruning_profile(&self, profile: &RuntimeProfile) {
        if self
            .iceberg_runtime_pruning_profile_flushed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let snapshot = self.iceberg_runtime_pruning_counters.snapshot();
        profile.counter_set_unit(
            "IcebergRuntimeFilePruning/FilesTotal",
            u64_to_i64_saturating(snapshot.files_total),
        );
        profile.counter_set_unit(
            "IcebergRuntimeFilePruning/FilesSelected",
            u64_to_i64_saturating(snapshot.files_selected),
        );
        profile.counter_set_unit(
            "IcebergRuntimeFilePruning/FilesPruned",
            u64_to_i64_saturating(snapshot.files_pruned),
        );
        profile.counter_set_unit(
            "IcebergRuntimeFilePruning/Predicates",
            u64_to_i64_saturating(snapshot.predicates),
        );
        profile.counter_set_unit(
            "IcebergRuntimeFilePruning/Unsupported",
            u64_to_i64_saturating(snapshot.unsupported),
        );
        profile.counter_set_unit(
            "IcebergRuntimeFilePruning/Unavailable",
            u64_to_i64_saturating(snapshot.unavailable),
        );
    }

    #[cfg(test)]
    fn iceberg_runtime_pruning_counter_snapshot_for_test(
        &self,
    ) -> HdfsIcebergRuntimePruningCounterSnapshot {
        self.iceberg_runtime_pruning_counters.snapshot()
    }
    fn late_prune_morsel_with_ordered_predicate(
        &self,
        morsel: &ScanMorsel,
        slot_id: SlotId,
        predicate: &NativeOrderedRangePredicate,
    ) -> Result<ScanMorselPruneDecision, String> {
        let Some(range) = morsel.file_range() else {
            return Ok(ScanMorselPruneDecision::Keep);
        };
        let Some(metadata) = range.iceberg_file_pruning.as_ref() else {
            return Ok(ScanMorselPruneDecision::Keep);
        };
        let Some(pruning) = self.cfg.iceberg_runtime_pruning.as_ref() else {
            return Ok(ScanMorselPruneDecision::Keep);
        };
        let Some(column) = pruning.slot_to_column.get(&slot_id) else {
            return Ok(ScanMorselPruneDecision::Keep);
        };
        let Some(stats) = metadata.columns.get(column).or_else(|| {
            metadata
                .columns
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(column))
                .map(|(_, stats)| stats)
        }) else {
            return Ok(ScanMorselPruneDecision::Keep);
        };
        let Some(candidates) = exact_ordered_file_candidates(
            stats,
            metadata.null_state(column),
            predicate.data_type(),
        ) else {
            return Ok(ScanMorselPruneDecision::Keep);
        };
        let Ok(mask) = predicate.evaluate(candidates.as_ref()) else {
            return Ok(ScanMorselPruneDecision::Keep);
        };
        if mask.iter().all(|value| value != Some(true)) {
            Ok(ScanMorselPruneDecision::Skip)
        } else {
            Ok(ScanMorselPruneDecision::Keep)
        }
    }

    #[cfg(test)]
    fn build_incremental_morsels(
        &self,
        scan_ranges: &[IncrementalScanRange],
    ) -> Result<ScanMorsels, String> {
        let mut morsels = Vec::new();
        let mut has_more = false;
        let expected_file_format = self.expected_hdfs_file_format();

        for scan_range in scan_ranges {
            if let Some(value) = scan_range.has_more() {
                has_more = value;
            }

            let IncrementalScanRange::Hdfs {
                range: hdfs_range, ..
            } = scan_range
            else {
                continue;
            };

            if let Some(expected) = expected_file_format {
                let file_format = hdfs_range.file_format.ok_or_else(|| {
                    "incremental hdfs scan range is missing file_format".to_string()
                })?;
                if file_format != expected {
                    return Err(format!(
                        "incremental hdfs scan range file_format mismatch: expected {:?}, got {:?}",
                        expected, file_format
                    ));
                }
            }

            let path = if let Some(path) = hdfs_range
                .full_path
                .as_ref()
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
            {
                path.to_string()
            } else if let Some(rel) = hdfs_range
                .relative_path
                .as_ref()
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
            {
                let table_id = hdfs_range.table_id.ok_or_else(|| {
                    "incremental hdfs scan range has relative_path but missing table_id".to_string()
                })?;
                let base = self
                    .cfg
                    .iceberg_table_locations
                    .get(&table_id)
                    .map(|s| s.trim_end_matches('/'))
                    .ok_or_else(|| {
                        format!(
                            "incremental hdfs scan range missing cached iceberg location for table_id={table_id}"
                        )
                    })?;
                let rel = rel.trim_start_matches('/');
                if rel.is_empty() {
                    base.to_string()
                } else {
                    format!("{base}/{rel}")
                }
            } else {
                return Err(
                    "incremental hdfs scan range requires non-empty full_path or relative_path"
                        .to_string(),
                );
            };

            let file_len = hdfs_range.file_length;
            let file_len = if file_len > 0 { file_len as u64 } else { 0 };
            let offset = hdfs_range.offset;
            let offset = if offset >= 0 { offset as u64 } else { 0 };
            let length = hdfs_range.length;
            let mut length = if length > 0 { length as u64 } else { 0 };
            if length == 0 && file_len > offset {
                length = file_len - offset;
            }

            let (scan_range_id, first_row_id) = if self.row_position_scan {
                let first_row_id = hdfs_range.first_row_id.ok_or_else(|| {
                    "incremental hdfs scan range missing first_row_id for row position scan"
                        .to_string()
                })?;
                (self.next_incremental_scan_range_id(), Some(first_row_id))
            } else {
                (-1, None)
            };

            let delete_files = self.lowered_delete_files_for_range(&path, offset, length)?;
            let ivm_change_op = hdfs_range.ivm_change_op;
            // data_sequence_number is not carried by FE incremental ranges.
            // It is populated at initial lowering time from
            // the Iceberg manifest entry for V3 row-lineage tables.
            let data_sequence_number: Option<i64> = None;
            morsels.push(ScanMorsel::FileRange {
                path,
                file_len,
                offset,
                length,
                scan_range_id,
                first_row_id,
                data_sequence_number,
                ivm_change_op,
                included_positions: None,
                external_datacache: hdfs_range.external_datacache.clone(),
                delete_files,
                iceberg_file_pruning: None,
            });
        }

        Ok(ScanMorsels::new(morsels, has_more))
    }

    #[cfg(test)]
    fn build_morsels(&self) -> Result<ScanMorsels, String> {
        self.build_morsels_from_ordered_ranges(self.ordered_initial_ranges())
    }
}

impl ConnectorReadCoreFacet for HdfsRuntimePruningFacet {
    fn flush_morsel_materialization_profile(&self, profile: &RuntimeProfile) {
        self.flush_iceberg_runtime_pruning_profile(profile);
    }

    fn late_prune_morsel_with_ordered_predicate(
        &self,
        morsel: &ScanMorsel,
        slot_id: SlotId,
        predicate: &NativeOrderedRangePredicate,
    ) -> Result<ScanMorselPruneDecision, String> {
        self.late_prune_morsel_with_ordered_predicate(morsel, slot_id, predicate)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};

    use crate::cache::{CacheOptions, DataCacheManager};
    use crate::common::ids::SlotId;
    use crate::common::min_max_predicate::{MinMaxPredicate, MinMaxPredicateValue};
    use crate::connector::iceberg::delete_file::{
        IcebergDeleteFileSpec, IcebergFileContent, IcebergFileFormat,
    };
    use crate::connector::iceberg::file_pruning::{
        IcebergFileNullState, IcebergFilePruningMetadata,
    };
    #[cfg(feature = "compat")]
    use crate::connector::iceberg::file_pruning_wire::{
        iceberg_file_pruning_metadata_from_thrift, iceberg_file_pruning_metadata_to_thrift,
    };
    use crate::connector::iceberg::scan_model::IcebergColumnStats;
    #[cfg(feature = "compat")]
    use crate::connector::iceberg::scan_model::IcebergDataFileInfo;
    use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
    use crate::exec::node::scan::{
        HdfsScanFileFormat, IncrementalHdfsScanRange, IncrementalScanRange, ScanMorsel,
        ScanMorselPruneDecision, ScanOp,
    };
    use crate::formats::parquet::{
        ParquetReadCachePolicy, ParquetScanConfig, ParquetSlotKind, VariantPathPruningPredicate,
    };
    use crate::fs::scan_context::FileScanRange;
    use crate::runtime_filter::exec::ordered_range_predicate::{
        NativeOrderedRangePredicate, OrderedRangePredicateContract,
    };
    use crate::runtime_filter::model::contract::{ChannelId, NullOrder, SortDirection};
    use crate::runtime_filter::port::identity::LogicalVersion;
    use crate::runtime_filter::port::ordered_bound::OrderedScalar;

    use super::{
        HdfsConnectorInstance, HdfsDeleteAuxiliary, HdfsIcebergRuntimePruningConfig,
        HdfsIncrementalSplitAdapter, HdfsInstanceConfig, HdfsRuntimePruningFacet, HdfsScanConfig,
        apply_parquet_pruning_gate_for_delete_files,
    };
    use crate::connector::runtime::{ConnectorReadAuxiliary, IncrementalConnectorSplitAdapter};

    fn plain_file_range(path: &str) -> FileScanRange {
        FileScanRange {
            path: path.to_string(),
            file_len: 1024,
            offset: 0,
            length: 1024,
            scan_range_id: -1,
            first_row_id: None,
            data_sequence_number: None,
            ivm_change_op: None,
            included_positions: None,
            external_datacache: None,
            delete_files: Vec::new(),
            iceberg_file_pruning: None,
        }
    }

    #[test]
    fn hdfs_delete_auxiliary_skips_ranges_without_delete_files() {
        let auxiliary = HdfsDeleteAuxiliary::new(None);
        let range = plain_file_range("s3://bucket/path/data.parquet");

        assert!(
            auxiliary
                .load_iceberg_position_deletes(&range)
                .expect("no delete files should not open storage")
                .is_none()
        );
        assert!(
            auxiliary
                .load_iceberg_equality_deletes(&range)
                .expect("no delete files should not open storage")
                .is_none()
        );
    }

    #[test]
    fn hdfs_incremental_adapter_registers_file_sidecars_before_reading() {
        let instance_id =
            novarocks_spi::connector::ConnectorInstanceId::parse("hdfs.test").expect("instance ID");
        let chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                Field::new("value", DataType::Int32, true),
                None,
                None,
            )])
            .expect("chunk schema"),
        );
        let provider = Arc::new(HdfsConnectorInstance::new(
            instance_id,
            HdfsInstanceConfig {
                scan: HdfsScanConfig {
                    ranges: vec![plain_file_range("s3://bucket/path/seed.parquet")],
                    original_range_count: 1,
                    has_more: true,
                    limit: None,
                    profile_label: None,
                    format: None,
                    object_store_config: None,
                    iceberg_table_locations: HashMap::new(),
                    query_global_dicts: Default::default(),
                    iceberg_runtime_pruning: None,
                },
                chunk_schema,
            },
        ));
        let adapter = HdfsIncrementalSplitAdapter {
            provider: Arc::clone(&provider),
        };

        let appended = adapter
            .prepare_incremental_ranges(&[
                make_hdfs_range("s3://bucket/path/next.parquet", None),
                make_end_marker(false),
            ])
            .expect("prepare HDFS range");
        let crate::connector::runtime::ConnectorSplitAppend::Scheduled {
            scheduled,
            has_more,
        } = &appended
        else {
            panic!("HDFS adapter must schedule file sidecars");
        };
        assert!(!has_more);
        assert_eq!(scheduled.len(), 1);
        assert_eq!(provider.range_count().expect("range count"), 1);
        adapter
            .commit_incremental_ranges(&appended)
            .expect("commit HDFS range");
        assert_eq!(provider.range_count().expect("range count"), 2);
        assert_eq!(
            provider.range_for_index(1).expect("registered range").path,
            "s3://bucket/path/next.parquet"
        );
    }

    fn make_hdfs_range(path: &str, first_row_id: Option<i64>) -> IncrementalScanRange {
        make_hdfs_range_with_change_op(path, first_row_id, None)
    }

    fn make_hdfs_range_with_change_op(
        path: &str,
        first_row_id: Option<i64>,
        ivm_change_op: Option<i8>,
    ) -> IncrementalScanRange {
        IncrementalScanRange::Hdfs {
            has_more: None,
            range: IncrementalHdfsScanRange {
                file_format: Some(HdfsScanFileFormat::Parquet),
                full_path: Some(path.to_string()),
                relative_path: None,
                table_id: None,
                file_length: 256,
                offset: 0,
                length: 100,
                first_row_id,
                ivm_change_op,
                external_datacache: None,
            },
        }
    }

    fn make_end_marker(has_more: bool) -> IncrementalScanRange {
        IncrementalScanRange::Empty {
            has_more: Some(has_more),
        }
    }

    fn test_datacache_context() -> crate::cache::DataCacheContext {
        let cache_options = CacheOptions::from_query_options(None).expect("cache options");
        DataCacheManager::instance().external_context(cache_options)
    }

    fn test_delete_file(file_content: IcebergFileContent) -> IcebergDeleteFileSpec {
        IcebergDeleteFileSpec {
            path: "delete.parquet".to_string(),
            file_format: IcebergFileFormat::Parquet,
            file_content,
            length: Some(1),
            content_offset: None,
            content_size_in_bytes: None,
        }
    }

    fn test_iceberg_file_pruning_metadata() -> IcebergFilePruningMetadata {
        IcebergFilePruningMetadata {
            columns: HashMap::from([(
                "id".to_string(),
                IcebergColumnStats {
                    null_count: None,
                    value_count: None,
                    column_size: None,
                    lower_bound: Some(10_i64.to_le_bytes().to_vec()),
                    upper_bound: Some(20_i64.to_le_bytes().to_vec()),
                },
            )]),
            null_states: HashMap::new(),
        }
    }

    fn iceberg_file_pruning_metadata_for_i32_range(
        column: &str,
        lower: i32,
        upper: i32,
    ) -> IcebergFilePruningMetadata {
        IcebergFilePruningMetadata {
            columns: HashMap::from([(
                column.to_string(),
                IcebergColumnStats {
                    null_count: None,
                    value_count: None,
                    column_size: None,
                    lower_bound: Some(lower.to_le_bytes().to_vec()),
                    upper_bound: Some(upper.to_le_bytes().to_vec()),
                },
            )]),
            null_states: HashMap::new(),
        }
    }

    fn exact_iceberg_file_pruning_metadata_for_i32_range(
        column: &str,
        lower: i32,
        upper: i32,
    ) -> IcebergFilePruningMetadata {
        let mut metadata = iceberg_file_pruning_metadata_for_i32_range(column, lower, upper);
        let stats = metadata
            .columns
            .get_mut(column)
            .expect("exact Iceberg column stats");
        stats.null_count = Some(0);
        stats.value_count = Some(2);
        metadata
    }

    fn ordered_i32_predicate(bound: i32) -> NativeOrderedRangePredicate {
        let order = crate::runtime_filter::exec::ordered_range_predicate::tests_support::contract(
            DataType::Int32,
            SortDirection::Ascending,
            NullOrder::Last,
        );
        let version = LogicalVersion::FIRST;
        let bundle = crate::runtime_filter::exec::ordered_range_predicate::tests_support::bundle(
            order.clone(),
            Some(OrderedScalar::Int32(bound)),
            version,
        );
        NativeOrderedRangePredicate::compile(
            &bundle,
            &OrderedRangePredicateContract::new(ChannelId::new(7), order, version)
                .expect("ordered predicate contract"),
        )
        .expect("ordered predicate")
    }

    #[cfg(feature = "compat")]
    fn ordered_i64_predicate(bound: i64) -> NativeOrderedRangePredicate {
        let order = crate::runtime_filter::exec::ordered_range_predicate::tests_support::contract(
            DataType::Int64,
            SortDirection::Ascending,
            NullOrder::Last,
        );
        let version = LogicalVersion::FIRST;
        let bundle = crate::runtime_filter::exec::ordered_range_predicate::tests_support::bundle(
            order.clone(),
            Some(OrderedScalar::Int64(bound)),
            version,
        );
        NativeOrderedRangePredicate::compile(
            &bundle,
            &OrderedRangePredicateContract::new(ChannelId::new(7), order, version)
                .expect("ordered predicate contract"),
        )
        .expect("ordered predicate")
    }

    fn ordered_utf8_predicate(bound: &str) -> NativeOrderedRangePredicate {
        let order = crate::runtime_filter::exec::ordered_range_predicate::tests_support::contract(
            DataType::Utf8,
            SortDirection::Ascending,
            NullOrder::Last,
        );
        let version = LogicalVersion::FIRST;
        let bundle = crate::runtime_filter::exec::ordered_range_predicate::tests_support::bundle(
            order.clone(),
            Some(OrderedScalar::Utf8(Arc::from(bound))),
            version,
        );
        NativeOrderedRangePredicate::compile(
            &bundle,
            &OrderedRangePredicateContract::new(ChannelId::new(7), order, version)
                .expect("ordered UTF8 predicate contract"),
        )
        .expect("ordered UTF8 predicate")
    }

    fn iceberg_file_range_for_runtime_pruning_test(
        path: &str,
        stats: Option<IcebergFilePruningMetadata>,
    ) -> FileScanRange {
        FileScanRange {
            path: path.to_string(),
            file_len: 1024,
            offset: 0,
            length: 1024,
            scan_range_id: -1,
            first_row_id: None,
            data_sequence_number: None,
            ivm_change_op: None,
            included_positions: None,
            external_datacache: None,
            delete_files: Vec::new(),
            iceberg_file_pruning: stats,
        }
    }

    fn hdfs_cfg_with_two_iceberg_files_for_test() -> HdfsScanConfig {
        HdfsScanConfig {
            ranges: vec![
                iceberg_file_range_for_runtime_pruning_test(
                    "s3://bucket/path/hit.parquet",
                    Some(iceberg_file_pruning_metadata_for_i32_range("k1", 90, 110)),
                ),
                iceberg_file_range_for_runtime_pruning_test(
                    "s3://bucket/path/miss.parquet",
                    Some(iceberg_file_pruning_metadata_for_i32_range("k1", 1, 10)),
                ),
            ],
            original_range_count: 2,
            has_more: false,
            limit: None,
            profile_label: None,
            format: None,
            object_store_config: None,
            iceberg_table_locations: std::collections::HashMap::new(),
            query_global_dicts: Default::default(),
            iceberg_runtime_pruning: Some(HdfsIcebergRuntimePruningConfig {
                slot_to_column: HashMap::from([(SlotId::new(3), "k1".to_string())]),
                min_max_filter_columns: HashMap::new(),
                discrete_set_max_values: 256,
            }),
        }
    }

    #[test]
    fn native_scan_ordered_live_hdfs_skips_only_exact_file_range_evidence() {
        let mut cfg = hdfs_cfg_with_two_iceberg_files_for_test();
        cfg.ranges = Vec::new();
        let op = HdfsRuntimePruningFacet::new(cfg);
        let predicate = ordered_i32_predicate(50);

        let exact_miss = iceberg_file_range_for_runtime_pruning_test(
            "s3://bucket/path/exact-miss.parquet",
            Some(exact_iceberg_file_pruning_metadata_for_i32_range(
                "k1", 90, 110,
            )),
        );
        let exact_miss = HdfsRuntimePruningFacet::new(HdfsScanConfig {
            ranges: vec![exact_miss],
            ..op.cfg.clone()
        })
        .build_morsels()
        .expect("exact morsel")
        .morsels
        .pop()
        .expect("exact file range");
        assert_eq!(
            op.late_prune_morsel_with_ordered_predicate(&exact_miss, SlotId::new(3), &predicate,)
                .expect("exact late prune"),
            ScanMorselPruneDecision::Skip
        );

        let mut explicit_no_nulls = iceberg_file_pruning_metadata_for_i32_range("k1", 90, 110);
        explicit_no_nulls
            .null_states
            .insert("k1".to_string(), IcebergFileNullState::NoNulls);
        let explicit_no_nulls = iceberg_file_range_for_runtime_pruning_test(
            "s3://bucket/path/explicit-no-nulls.parquet",
            Some(explicit_no_nulls),
        );
        let explicit_no_nulls = HdfsRuntimePruningFacet::new(HdfsScanConfig {
            ranges: vec![explicit_no_nulls],
            ..op.cfg.clone()
        })
        .build_morsels()
        .expect("explicit-null-state morsel")
        .morsels
        .pop()
        .expect("explicit-null-state file range");
        assert_eq!(
            op.late_prune_morsel_with_ordered_predicate(
                &explicit_no_nulls,
                SlotId::new(3),
                &predicate,
            )
            .expect("explicit-null-state late prune"),
            ScanMorselPruneDecision::Skip
        );

        let missing_counts = iceberg_file_range_for_runtime_pruning_test(
            "s3://bucket/path/missing-counts.parquet",
            Some(iceberg_file_pruning_metadata_for_i32_range("k1", 90, 110)),
        );
        let missing_counts = HdfsRuntimePruningFacet::new(HdfsScanConfig {
            ranges: vec![missing_counts],
            ..op.cfg.clone()
        })
        .build_morsels()
        .expect("missing-counts morsel")
        .morsels
        .pop()
        .expect("missing-counts file range");
        assert_eq!(
            op.late_prune_morsel_with_ordered_predicate(
                &missing_counts,
                SlotId::new(3),
                &predicate,
            )
            .expect("missing-counts late prune"),
            ScanMorselPruneDecision::Keep
        );

        let missing_metadata = HdfsRuntimePruningFacet::new(HdfsScanConfig {
            ranges: vec![iceberg_file_range_for_runtime_pruning_test(
                "s3://bucket/path/missing-metadata.parquet",
                None,
            )],
            ..op.cfg.clone()
        })
        .build_morsels()
        .expect("missing-metadata morsel")
        .morsels
        .pop()
        .expect("missing-metadata file range");
        assert_eq!(
            op.late_prune_morsel_with_ordered_predicate(
                &missing_metadata,
                SlotId::new(3),
                &predicate,
            )
            .expect("missing-metadata late prune"),
            ScanMorselPruneDecision::Keep
        );

        let unsupported_metadata = IcebergFilePruningMetadata {
            columns: HashMap::from([(
                "k1".to_string(),
                IcebergColumnStats {
                    null_count: Some(0),
                    value_count: Some(2),
                    column_size: None,
                    lower_bound: Some(b"z".to_vec()),
                    upper_bound: Some(b"zz".to_vec()),
                },
            )]),
            null_states: HashMap::new(),
        };
        let unsupported = HdfsRuntimePruningFacet::new(HdfsScanConfig {
            ranges: vec![iceberg_file_range_for_runtime_pruning_test(
                "s3://bucket/path/unsupported-string-bounds.parquet",
                Some(unsupported_metadata),
            )],
            ..op.cfg.clone()
        })
        .build_morsels()
        .expect("unsupported morsel")
        .morsels
        .pop()
        .expect("unsupported file range");
        assert_eq!(
            op.late_prune_morsel_with_ordered_predicate(
                &unsupported,
                SlotId::new(3),
                &ordered_utf8_predicate("m"),
            )
            .expect("unsupported late prune"),
            ScanMorselPruneDecision::Keep
        );
        assert_eq!(
            op.late_prune_morsel_with_ordered_predicate(
                &ScanMorsel::Schema {
                    table_name: "not-a-file-range".to_string(),
                },
                SlotId::new(3),
                &predicate,
            )
            .expect("non-file late prune"),
            ScanMorselPruneDecision::Keep
        );
    }

    #[cfg(feature = "compat")]
    #[test]
    fn thrift_missing_null_count_keeps_ordered_late_pruning_conservative_after_wire_roundtrip() {
        let mut file =
            IcebergDataFileInfo::for_test("s3://bucket/path/missing-null-count.parquet", 1024, 2);
        file.column_stats = Some(HashMap::from([(
            "k1".to_string(),
            IcebergColumnStats {
                null_count: None,
                value_count: Some(2),
                column_size: None,
                lower_bound: Some(90_i64.to_le_bytes().to_vec()),
                upper_bound: Some(110_i64.to_le_bytes().to_vec()),
            },
        )]));
        let columns = vec![novarocks_catalog::schema::ColumnDef {
            name: "k1".to_string(),
            data_type: DataType::Int64,
            nullable: true,
            write_default: None,
            logical_type: None,
        }];
        let encoded = iceberg_file_pruning_metadata_to_thrift(&file, &columns);
        let wire = crate::thrift::plan_nodes::THdfsScanRange {
            min_max_values: encoded,
            ..Default::default()
        };
        let decoded = iceberg_file_pruning_metadata_from_thrift(&wire, &["k1".to_string()]);
        let range = iceberg_file_range_for_runtime_pruning_test(
            "s3://bucket/path/missing-null-count.parquet",
            decoded,
        );
        let mut cfg = hdfs_cfg_with_two_iceberg_files_for_test();
        cfg.ranges = vec![range];
        let op = HdfsRuntimePruningFacet::new(cfg);
        let morsel = op
            .build_morsels()
            .expect("thrift roundtrip morsel")
            .morsels
            .pop()
            .expect("thrift file morsel");
        assert_eq!(
            op.late_prune_morsel_with_ordered_predicate(
                &morsel,
                SlotId::new(3),
                &ordered_i64_predicate(50),
            )
            .expect("thrift late prune"),
            ScanMorselPruneDecision::Keep,
            "missing null_count must not be encoded as explicit NoNulls"
        );
    }

    fn hdfs_cfg_with_all_pruned_iceberg_files_for_test() -> HdfsScanConfig {
        HdfsScanConfig {
            ranges: vec![
                iceberg_file_range_for_runtime_pruning_test(
                    "s3://bucket/path/miss-a.parquet",
                    Some(iceberg_file_pruning_metadata_for_i32_range("k1", 1, 10)),
                ),
                iceberg_file_range_for_runtime_pruning_test(
                    "s3://bucket/path/miss-b.parquet",
                    Some(iceberg_file_pruning_metadata_for_i32_range("k1", 20, 30)),
                ),
            ],
            original_range_count: 2,
            has_more: false,
            limit: None,
            profile_label: None,
            format: None,
            object_store_config: None,
            iceberg_table_locations: std::collections::HashMap::new(),
            query_global_dicts: Default::default(),
            iceberg_runtime_pruning: Some(HdfsIcebergRuntimePruningConfig {
                slot_to_column: HashMap::from([(SlotId::new(3), "k1".to_string())]),
                min_max_filter_columns: HashMap::new(),
                discrete_set_max_values: 256,
            }),
        }
    }

    fn hdfs_cfg_with_two_iceberg_files_without_metadata_for_test() -> HdfsScanConfig {
        HdfsScanConfig {
            ranges: vec![
                iceberg_file_range_for_runtime_pruning_test("s3://bucket/path/hit.parquet", None),
                iceberg_file_range_for_runtime_pruning_test("s3://bucket/path/miss.parquet", None),
            ],
            original_range_count: 2,
            has_more: false,
            limit: None,
            profile_label: None,
            format: None,
            object_store_config: None,
            iceberg_table_locations: std::collections::HashMap::new(),
            query_global_dicts: Default::default(),
            iceberg_runtime_pruning: Some(HdfsIcebergRuntimePruningConfig {
                slot_to_column: HashMap::from([(SlotId::new(3), "k1".to_string())]),
                min_max_filter_columns: HashMap::new(),
                discrete_set_max_values: 256,
            }),
        }
    }

    fn test_prunable_parquet_config() -> ParquetScanConfig {
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            &Schema::new(vec![
                Field::new("id", DataType::Int32, true),
                Field::new("__nr_var_payload_a", DataType::Int64, true),
                Field::new("payload", DataType::LargeBinary, true),
            ]),
            &[SlotId::new(1), SlotId::new(2), SlotId::new(3)],
        )
        .expect("chunk schema");
        ParquetScanConfig {
            columns: vec!["id".to_string(), "payload".to_string()],
            chunk_schema,
            slot_kinds: vec![
                ParquetSlotKind::Regular,
                ParquetSlotKind::Regular,
                ParquetSlotKind::Variant,
            ],
            case_sensitive: true,
            enable_page_index: true,
            min_max_predicates: vec![MinMaxPredicate::Gt {
                column: "0".to_string(),
                value: MinMaxPredicateValue::Int32(5),
            }],
            runtime_min_max_filter_columns: std::collections::HashMap::new(),
            variant_path_predicates: vec![VariantPathPruningPredicate {
                output_slot_id: SlotId::new(2),
                source_slot_id: SlotId::new(3),
                source_field_id: Some(10),
                canonical_path: "$.a".to_string(),
                requested_type: DataType::Int64,
                predicate: MinMaxPredicate::Gt {
                    column: "__nr_var_payload_a".to_string(),
                    value: MinMaxPredicateValue::Int64(7),
                },
            }],
            batch_size: Some(1024),
            datacache: test_datacache_context(),
            cache_policy: ParquetReadCachePolicy::with_flags(false, false, None),
            profile_label: None,
            iceberg_output_schema: Some(Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, true),
                Field::new("payload", DataType::LargeBinary, true),
            ]))),
            variant_path_columns: Vec::new(),
            query_global_dicts: Default::default(),
        }
    }

    #[test]
    fn hdfs_scan_position_delete_morsel_strips_parquet_pruning() {
        let mut parquet_cfg = test_prunable_parquet_config();
        parquet_cfg
            .runtime_min_max_filter_columns
            .insert(11, "id".to_string());

        apply_parquet_pruning_gate_for_delete_files(
            &mut parquet_cfg,
            &[test_delete_file(IcebergFileContent::PositionDeletes)],
        );

        assert!(!parquet_cfg.enable_page_index);
        assert!(parquet_cfg.min_max_predicates.is_empty());
        assert!(parquet_cfg.runtime_min_max_filter_columns.is_empty());
        assert!(parquet_cfg.variant_path_predicates.is_empty());
    }

    #[test]
    fn hdfs_scan_equality_delete_morsel_keeps_parquet_pruning() {
        let mut parquet_cfg = test_prunable_parquet_config();

        apply_parquet_pruning_gate_for_delete_files(
            &mut parquet_cfg,
            &[test_delete_file(IcebergFileContent::EqualityDeletes)],
        );

        assert!(parquet_cfg.enable_page_index);
        assert_eq!(parquet_cfg.min_max_predicates.len(), 1);
        assert_eq!(parquet_cfg.variant_path_predicates.len(), 1);
    }

    #[test]
    fn incremental_hdfs_ranges_parse_data_and_end_marker() {
        let cfg = HdfsScanConfig {
            ranges: vec![],
            original_range_count: 0,
            has_more: true,
            limit: None,
            profile_label: None,
            format: None,
            object_store_config: None,
            iceberg_table_locations: std::collections::HashMap::new(),
            query_global_dicts: Default::default(),
            iceberg_runtime_pruning: None,
        };
        let op = HdfsRuntimePruningFacet::new(cfg);

        let morsels = op
            .build_incremental_morsels(&[
                make_hdfs_range("s3://bucket/path/file.parquet", None),
                make_end_marker(false),
            ])
            .expect("build incremental morsels");

        assert!(!morsels.has_more);
        assert_eq!(morsels.morsels.len(), 1);
        match &morsels.morsels[0] {
            ScanMorsel::FileRange {
                path,
                scan_range_id,
                ..
            } => {
                assert_eq!(path, "s3://bucket/path/file.parquet");
                assert_eq!(*scan_range_id, -1);
            }
            other => panic!("unexpected morsel: {:?}", other),
        }
    }

    #[test]
    fn incremental_hdfs_ranges_assign_row_position_scan_range_id_contiguously() {
        let cfg = HdfsScanConfig {
            ranges: vec![FileScanRange {
                path: "s3://bucket/path/seed.parquet".to_string(),
                file_len: 100,
                offset: 0,
                length: 100,
                scan_range_id: 7,
                first_row_id: Some(10),
                data_sequence_number: None,
                ivm_change_op: None,
                included_positions: None,
                external_datacache: None,
                delete_files: Vec::new(),
                iceberg_file_pruning: None,
            }],
            original_range_count: 1,
            has_more: true,
            limit: None,
            profile_label: None,
            format: None,
            object_store_config: None,
            iceberg_table_locations: std::collections::HashMap::new(),
            query_global_dicts: Default::default(),
            iceberg_runtime_pruning: None,
        };
        let op = HdfsRuntimePruningFacet::new(cfg);

        let morsels = op
            .build_incremental_morsels(&[
                make_hdfs_range("s3://bucket/path/a.parquet", Some(1000)),
                make_hdfs_range("s3://bucket/path/b.parquet", Some(2000)),
                make_end_marker(false),
            ])
            .expect("build incremental morsels");

        assert!(!morsels.has_more);
        assert_eq!(morsels.morsels.len(), 2);
        match &morsels.morsels[0] {
            ScanMorsel::FileRange {
                scan_range_id,
                first_row_id,
                ..
            } => {
                assert_eq!(*scan_range_id, 8);
                assert_eq!(*first_row_id, Some(1000));
            }
            other => panic!("unexpected morsel: {:?}", other),
        }
        match &morsels.morsels[1] {
            ScanMorsel::FileRange {
                scan_range_id,
                first_row_id,
                ..
            } => {
                assert_eq!(*scan_range_id, 9);
                assert_eq!(*first_row_id, Some(2000));
            }
            other => panic!("unexpected morsel: {:?}", other),
        }
    }

    #[test]
    fn incremental_hdfs_ranges_reuse_lowered_delete_files() {
        let cfg = HdfsScanConfig {
            ranges: vec![FileScanRange {
                path: "s3://bucket/path/file.parquet".to_string(),
                file_len: 100,
                offset: 0,
                length: 100,
                scan_range_id: -1,
                first_row_id: None,
                data_sequence_number: None,
                ivm_change_op: None,
                included_positions: None,
                external_datacache: None,
                delete_files: vec![test_delete_file(IcebergFileContent::PositionDeletes)],
                iceberg_file_pruning: None,
            }],
            original_range_count: 1,
            has_more: true,
            limit: None,
            profile_label: None,
            format: None,
            object_store_config: None,
            iceberg_table_locations: std::collections::HashMap::new(),
            query_global_dicts: Default::default(),
            iceberg_runtime_pruning: None,
        };
        let op = HdfsRuntimePruningFacet::new(cfg);

        let morsels = op
            .build_incremental_morsels(&[
                make_hdfs_range("s3://bucket/path/file.parquet", None),
                make_end_marker(false),
            ])
            .expect("build incremental morsels");

        match &morsels.morsels[0] {
            ScanMorsel::FileRange { delete_files, .. } => {
                assert_eq!(delete_files.len(), 1);
                assert_eq!(
                    delete_files[0].file_content,
                    IcebergFileContent::PositionDeletes
                );
            }
            other => panic!("unexpected morsel: {:?}", other),
        }
    }

    #[test]
    fn incremental_hdfs_ranges_reject_same_path_delete_files_without_exact_match() {
        let cfg = HdfsScanConfig {
            ranges: vec![FileScanRange {
                path: "s3://bucket/path/file.parquet".to_string(),
                file_len: 100,
                offset: 64,
                length: 100,
                scan_range_id: -1,
                first_row_id: None,
                data_sequence_number: None,
                ivm_change_op: None,
                included_positions: None,
                external_datacache: None,
                delete_files: vec![test_delete_file(IcebergFileContent::PositionDeletes)],
                iceberg_file_pruning: None,
            }],
            original_range_count: 1,
            has_more: true,
            limit: None,
            profile_label: None,
            format: None,
            object_store_config: None,
            iceberg_table_locations: std::collections::HashMap::new(),
            query_global_dicts: Default::default(),
            iceberg_runtime_pruning: None,
        };
        let op = HdfsRuntimePruningFacet::new(cfg);

        let err = op
            .build_incremental_morsels(&[make_hdfs_range("s3://bucket/path/file.parquet", None)])
            .expect_err("same-path delete files without exact lowered range must fail closed");

        assert!(err.contains("cannot safely reuse lowered Iceberg delete files"));
        assert!(err.contains("s3://bucket/path/file.parquet"));
        assert!(err.contains("offset=0"));
        assert!(err.contains("length=100"));
    }

    #[test]
    fn incremental_hdfs_ranges_allow_empty_delete_files_without_same_path_delete_files() {
        let cfg = HdfsScanConfig {
            ranges: vec![FileScanRange {
                path: "s3://bucket/path/other.parquet".to_string(),
                file_len: 100,
                offset: 0,
                length: 100,
                scan_range_id: -1,
                first_row_id: None,
                data_sequence_number: None,
                ivm_change_op: None,
                included_positions: None,
                external_datacache: None,
                delete_files: vec![test_delete_file(IcebergFileContent::PositionDeletes)],
                iceberg_file_pruning: None,
            }],
            original_range_count: 1,
            has_more: true,
            limit: None,
            profile_label: None,
            format: None,
            object_store_config: None,
            iceberg_table_locations: std::collections::HashMap::new(),
            query_global_dicts: Default::default(),
            iceberg_runtime_pruning: None,
        };
        let op = HdfsRuntimePruningFacet::new(cfg);

        let morsels = op
            .build_incremental_morsels(&[make_hdfs_range("s3://bucket/path/file.parquet", None)])
            .expect("build incremental morsels");

        match &morsels.morsels[0] {
            ScanMorsel::FileRange { delete_files, .. } => {
                assert!(delete_files.is_empty());
            }
            other => panic!("unexpected morsel: {:?}", other),
        }
    }

    #[test]
    fn incremental_hdfs_ranges_propagate_change_op_extended_column() {
        let cfg = HdfsScanConfig {
            ranges: vec![],
            original_range_count: 0,
            has_more: true,
            limit: None,
            profile_label: None,
            format: None,
            object_store_config: None,
            iceberg_table_locations: std::collections::HashMap::new(),
            query_global_dicts: Default::default(),
            iceberg_runtime_pruning: None,
        };
        let op = HdfsRuntimePruningFacet::new(cfg);

        let morsels = op
            .build_incremental_morsels(&[make_hdfs_range_with_change_op(
                "s3://bucket/path/file.parquet",
                None,
                Some(-1),
            )])
            .expect("build incremental morsels");

        assert_eq!(morsels.morsels.len(), 1);
        match &morsels.morsels[0] {
            ScanMorsel::FileRange { ivm_change_op, .. } => {
                assert_eq!(*ivm_change_op, Some(-1));
            }
            other => panic!("unexpected morsel: {:?}", other),
        }
    }

    #[test]
    fn build_morsels_prioritizes_large_plain_ranges() {
        let cfg = HdfsScanConfig {
            ranges: vec![
                FileScanRange {
                    path: "s3://bucket/path/small-a.parquet".to_string(),
                    file_len: 1024,
                    offset: 0,
                    length: 1024,
                    scan_range_id: -1,
                    first_row_id: None,
                    data_sequence_number: None,
                    ivm_change_op: None,
                    included_positions: None,
                    external_datacache: None,
                    delete_files: Vec::new(),
                    iceberg_file_pruning: None,
                },
                FileScanRange {
                    path: "s3://bucket/path/large.parquet".to_string(),
                    file_len: 128 * 1024 * 1024,
                    offset: 0,
                    length: 128 * 1024 * 1024,
                    scan_range_id: -1,
                    first_row_id: None,
                    data_sequence_number: None,
                    ivm_change_op: None,
                    included_positions: None,
                    external_datacache: None,
                    delete_files: Vec::new(),
                    iceberg_file_pruning: None,
                },
                FileScanRange {
                    path: "s3://bucket/path/small-b.parquet".to_string(),
                    file_len: 2048,
                    offset: 0,
                    length: 2048,
                    scan_range_id: -1,
                    first_row_id: None,
                    data_sequence_number: None,
                    ivm_change_op: None,
                    included_positions: None,
                    external_datacache: None,
                    delete_files: Vec::new(),
                    iceberg_file_pruning: None,
                },
            ],
            original_range_count: 3,
            has_more: false,
            limit: None,
            profile_label: None,
            format: None,
            object_store_config: None,
            iceberg_table_locations: std::collections::HashMap::new(),
            query_global_dicts: Default::default(),
            iceberg_runtime_pruning: None,
        };
        let op = HdfsRuntimePruningFacet::new(cfg);

        let morsels = op.build_morsels().expect("build morsels");

        let paths = morsels
            .morsels
            .iter()
            .map(|morsel| match morsel {
                ScanMorsel::FileRange { path, .. } => path.as_str(),
                other => panic!("unexpected morsel: {:?}", other),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            paths,
            vec![
                "s3://bucket/path/large.parquet",
                "s3://bucket/path/small-b.parquet",
                "s3://bucket/path/small-a.parquet",
            ]
        );
    }

    #[test]
    fn build_morsels_preserves_iceberg_file_pruning_metadata() {
        let cfg = HdfsScanConfig {
            ranges: vec![FileScanRange {
                path: "s3://bucket/path/file.parquet".to_string(),
                file_len: 1024,
                offset: 0,
                length: 1024,
                scan_range_id: -1,
                first_row_id: None,
                data_sequence_number: None,
                ivm_change_op: None,
                included_positions: None,
                external_datacache: None,
                delete_files: Vec::new(),
                iceberg_file_pruning: Some(test_iceberg_file_pruning_metadata()),
            }],
            original_range_count: 1,
            has_more: false,
            limit: None,
            profile_label: None,
            format: None,
            object_store_config: None,
            iceberg_table_locations: std::collections::HashMap::new(),
            query_global_dicts: Default::default(),
            iceberg_runtime_pruning: None,
        };
        let op = HdfsRuntimePruningFacet::new(cfg);

        let morsels = op.build_morsels().expect("build morsels");

        match &morsels.morsels[0] {
            ScanMorsel::FileRange {
                iceberg_file_pruning,
                ..
            } => {
                let metadata = iceberg_file_pruning
                    .as_ref()
                    .expect("iceberg pruning metadata");
                assert_eq!(
                    metadata.columns["id"].upper_bound,
                    Some(20_i64.to_le_bytes().to_vec())
                );
            }
            other => panic!("unexpected morsel: {:?}", other),
        }
    }
}
