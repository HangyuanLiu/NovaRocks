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
use std::time::Instant;

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
        let _: SplitPayload = decode_payload(split.payload(), "split")?;
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "Iceberg SPI readers are enabled with the generic connector runtime",
        ))
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
