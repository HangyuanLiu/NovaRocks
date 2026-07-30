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

//! Provider-owned JDBC/MySQL SPI adapter.
//!
//! Credentials and connection configuration live only in this instance. The
//! transported table/scan/split payloads contain no connection URI, secret, or
//! core execution type.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorBatchBudget, ConnectorBatchReader, ConnectorBeginScanRequest, ConnectorCancellation,
    ConnectorError, ConnectorErrorKind, ConnectorInstance, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorOpenReaderRequest, ConnectorProviderId, ConnectorRead,
    ConnectorReadSelector, ConnectorRequestContext, ConnectorScan, ConnectorScanHandle,
    ConnectorSplit, ConnectorSplitPlanningRequest, ConnectorTableHandle,
    MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
};
use serde::{Deserialize, Serialize};

use super::{JdbcScanConfig, read_jdbc_batch};
use crate::connector::ConnectorRegistry;
use crate::connector::runtime::ConnectorReadScanSource;
use crate::exec::chunk::ChunkSchema;
use crate::exec::node::scan::ScanSource;
use crate::runtime::query_context::{QueryId, query_context_manager};
use crate::runtime::query_options::{QueryOptions, query_expire_durations};

const PROVIDER_ID: &str = "jdbc";
const PAYLOAD_VERSION: u8 = 1;

#[derive(Clone)]
pub struct JdbcInstanceConfig {
    pub scan: JdbcScanConfig,
}

pub(crate) struct JdbcConnectorInstance {
    instance_id: ConnectorInstanceId,
    config: JdbcInstanceConfig,
}

#[derive(Serialize, Deserialize)]
struct TablePayload {
    version: u8,
}

#[derive(Serialize, Deserialize)]
struct ScanPayload {
    version: u8,
    projection: Vec<usize>,
    limit: Option<u64>,
}

impl JdbcConnectorInstance {
    pub(crate) fn new(instance_id: ConnectorInstanceId, config: JdbcInstanceConfig) -> Self {
        Self {
            instance_id,
            config,
        }
    }

    pub(crate) fn connector_instance(self: Arc<Self>) -> Result<ConnectorInstance, ConnectorError> {
        ConnectorInstance::try_new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse(PROVIDER_ID)?,
                instance_id: self.instance_id.clone(),
            },
            None,
            self,
        )
    }

    pub(crate) fn table_handle(&self) -> Result<ConnectorTableHandle, ConnectorError> {
        encode_handle(
            &self.instance_id,
            &TablePayload {
                version: PAYLOAD_VERSION,
            },
        )
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

    fn projected_schema(&self, projection: &[usize]) -> Result<SchemaRef, ConnectorError> {
        let schema = self.config.scan.chunk_schema.arrow_schema_ref();
        let fields = projection
            .iter()
            .map(|index| {
                schema.fields().get(*index).cloned().ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        format!("JDBC projection index {index} is outside the output schema"),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Arc::new(Schema::new(fields)))
    }

    fn projected_config(&self, scan: &ScanPayload) -> Result<JdbcScanConfig, ConnectorError> {
        let schema = self.projected_schema(&scan.projection)?;
        let columns = scan
            .projection
            .iter()
            .map(|index| {
                self.config
                    .scan
                    .columns
                    .get(*index)
                    .cloned()
                    .ok_or_else(|| {
                        ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            format!("JDBC projection index {index} has no source column"),
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let slots = scan
            .projection
            .iter()
            .map(|index| {
                self.config
                    .scan
                    .chunk_schema
                    .slots()
                    .get(*index)
                    .cloned()
                    .ok_or_else(|| {
                        ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            format!("JDBC projection index {index} has no slot schema"),
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let chunk_schema = Arc::new(ChunkSchema::try_new(slots).map_err(internal)?);
        if chunk_schema.arrow_schema_ref().as_ref() != schema.as_ref() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "JDBC projected schema does not match its slot schema",
            ));
        }
        let mut config = self.config.scan.clone();
        config.columns = columns;
        config.chunk_schema = chunk_schema;
        let request_limit = scan.limit.and_then(|limit| usize::try_from(limit).ok());
        config.limit = match (config.limit, request_limit) {
            (Some(config_limit), Some(request_limit)) => Some(config_limit.min(request_limit)),
            (Some(config_limit), None) => Some(config_limit),
            (None, request_limit) => request_limit,
        };
        Ok(config)
    }
}

/// Build a generic SPI scan source for one decoder-local JDBC/MySQL instance.
///
/// Connection details stay in the provider instance. The returned source owns
/// the instance directly; its scan and split payloads are provider-owned JSON
/// without credentials.
pub(crate) fn plan_jdbc_read_source(
    _connectors: &ConnectorRegistry,
    instance_id: ConnectorInstanceId,
    config: JdbcInstanceConfig,
    batch: ConnectorBatchBudget,
    context: ConnectorRequestContext,
    target_parallelism: NonZeroUsize,
) -> Result<Arc<dyn ScanSource>, ConnectorError> {
    let chunk_schema = Arc::clone(&config.scan.chunk_schema);
    if config.scan.columns.len() != chunk_schema.slots().len() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "JDBC source columns must exactly match the decoded output schema",
        ));
    }
    let projection = (0..config.scan.columns.len()).collect::<Vec<_>>();
    let limit = config.scan.limit.map(|limit| limit as u64);
    let provider = Arc::new(JdbcConnectorInstance::new(instance_id, config));
    let table = provider.table_handle()?;
    let scan = provider.begin_scan(
        &table,
        ConnectorBeginScanRequest {
            projection,
            selector: ConnectorReadSelector::Current,
            limit,
            batch,
            context: context.clone(),
        },
    )?;
    let splits = provider.plan_splits(
        &scan.handle,
        ConnectorSplitPlanningRequest {
            target_parallelism,
            max_split_bytes: None,
            context: context.clone(),
        },
    )?;
    let instance = provider.connector_instance()?;
    Ok(Arc::new(ConnectorReadScanSource::new(
        Arc::new(instance),
        splits,
        ConnectorOpenReaderRequest {
            expected_schema: scan.output_schema,
            batch,
            context,
        },
        chunk_schema,
    )))
}

struct QueryCancellation {
    query_id: QueryId,
}

impl ConnectorCancellation for QueryCancellation {
    fn is_cancelled(&self) -> bool {
        query_context_manager().is_query_canceled(self.query_id)
    }
}

/// Construct the bounded read request used by the StarRocks JDBC and MySQL
/// decoders. The cancellation probe is local to the BE and never enters a
/// handle payload.
pub fn plan_starrocks_jdbc_read_source(
    connectors: &ConnectorRegistry,
    query_id: QueryId,
    node_id: i32,
    config: JdbcInstanceConfig,
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
        Arc::new(QueryCancellation { query_id }),
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )?;
    let target_parallelism = query_options
        .connector_io_tasks_per_scan_operator
        .and_then(|value| usize::try_from(value).ok())
        .and_then(NonZeroUsize::new)
        .unwrap_or_else(|| NonZeroUsize::new(1).expect("one is nonzero"));
    let instance_id = ConnectorInstanceId::parse(&format!("jdbc.{query_id}.{node_id}"))?;
    plan_jdbc_read_source(
        connectors,
        instance_id,
        config,
        batch,
        context,
        target_parallelism,
    )
}

impl ConnectorRead for JdbcConnectorInstance {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }

    fn begin_scan(
        &self,
        table: &ConnectorTableHandle,
        request: ConnectorBeginScanRequest,
    ) -> Result<ConnectorScan, ConnectorError> {
        self.validate_context(&request.context)?;
        ensure_owner(table.owner(), &self.instance_id)?;
        let table: TablePayload = decode_handle(table.payload())?;
        ensure_version(table.version, "table handle")?;
        let schema = self.projected_schema(&request.projection)?;
        let limit = request.limit.and_then(|limit| u64::try_from(limit).ok());
        let payload = ScanPayload {
            version: PAYLOAD_VERSION,
            projection: request.projection,
            limit,
        };
        let handle = encode_scan_handle(&self.instance_id, &payload)?;
        ensure_payload_budget(handle.payload(), &request.context, "JDBC scan handle")?;
        Ok(ConnectorScan {
            handle,
            output_schema: schema,
        })
    }

    fn plan_splits(
        &self,
        scan: &ConnectorScanHandle,
        request: ConnectorSplitPlanningRequest,
    ) -> Result<Vec<ConnectorSplit>, ConnectorError> {
        self.validate_context(&request.context)?;
        ensure_owner(scan.owner(), &self.instance_id)?;
        let scan_payload = scan.payload().clone();
        let scan: ScanPayload = decode_handle(&scan_payload)?;
        ensure_version(scan.version, "scan handle")?;
        let split =
            ConnectorSplit::try_new(self.instance_id.clone(), "jdbc-0", scan_payload, None)?;
        ensure_payload_budget(split.payload(), &request.context, "JDBC split")?;
        Ok(vec![split])
    }

    fn open_reader(
        &self,
        split: &ConnectorSplit,
        request: ConnectorOpenReaderRequest,
    ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
        self.validate_context(&request.context)?;
        ensure_owner(split.owner(), &self.instance_id)?;
        let scan: ScanPayload = decode_handle(split.payload())?;
        ensure_version(scan.version, "split")?;
        let config = self.projected_config(&scan)?;
        let schema = self.projected_schema(&scan.projection)?;
        if schema.as_ref() != request.expected_schema.as_ref() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "JDBC reader expected schema does not match its scan projection",
            ));
        }
        let batch = read_jdbc_batch(&config).map_err(internal)?;
        Ok(Box::new(JdbcBatchReader {
            batch: Some(batch),
            context: request.context,
            max_rows: request.batch.max_rows.get(),
            max_bytes: request.batch.max_bytes.get(),
            closed: false,
        }))
    }
}

struct JdbcBatchReader {
    batch: Option<RecordBatch>,
    context: novarocks_spi::connector::ConnectorRequestContext,
    max_rows: usize,
    max_bytes: usize,
    closed: bool,
}

impl ConnectorBatchReader for JdbcBatchReader {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        if self.closed {
            return Ok(None);
        }
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
        match self.batch.take() {
            Some(batch) => {
                if batch.num_rows() > self.max_rows
                    || batch.get_array_memory_size() > self.max_bytes
                {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::ResourceExhausted,
                        "JDBC reader exceeded its batch budget",
                    ));
                }
                Ok(Some(batch))
            }
            None => {
                self.closed = true;
                Ok(None)
            }
        }
    }
    fn close(&mut self) -> Result<(), ConnectorError> {
        self.closed = true;
        Ok(())
    }
}

fn ensure_owner(
    actual: &ConnectorInstanceId,
    expected: &ConnectorInstanceId,
) -> Result<(), ConnectorError> {
    if actual == expected {
        Ok(())
    } else {
        Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector handle owner does not match the JDBC instance",
        ))
    }
}

fn encode_handle<T: Serialize>(
    owner: &ConnectorInstanceId,
    value: &T,
) -> Result<ConnectorTableHandle, ConnectorError> {
    ConnectorTableHandle::try_new(
        owner.clone(),
        Bytes::from(serde_json::to_vec(value).map_err(internal)?),
    )
}
fn encode_scan_handle(
    owner: &ConnectorInstanceId,
    value: &ScanPayload,
) -> Result<ConnectorScanHandle, ConnectorError> {
    ConnectorScanHandle::try_new(
        owner.clone(),
        Bytes::from(serde_json::to_vec(value).map_err(internal)?),
    )
}
fn decode_handle<T: for<'de> Deserialize<'de>>(payload: &Bytes) -> Result<T, ConnectorError> {
    let value = serde_json::from_slice(payload).map_err(internal)?;
    Ok(value)
}

fn ensure_version(version: u8, subject: &str) -> Result<(), ConnectorError> {
    if version == PAYLOAD_VERSION {
        Ok(())
    } else {
        Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            format!("unsupported JDBC {subject} payload version {version}"),
        ))
    }
}

fn ensure_payload_budget(
    payload: &Bytes,
    context: &novarocks_spi::connector::ConnectorRequestContext,
    subject: &str,
) -> Result<(), ConnectorError> {
    if payload.len() > context.max_handle_payload_bytes()
        || payload.len() > context.max_total_payload_bytes()
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            format!("{subject} exceeds the connector payload budget"),
        ));
    }
    Ok(())
}
fn internal(error: impl std::fmt::Display) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, error.to_string())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow::datatypes::{DataType, Field};
    use novarocks_spi::connector::{
        ConnectorBatchBudget, ConnectorBeginScanRequest, ConnectorCancellation,
        ConnectorOpenReaderRequest, ConnectorReadSelector, ConnectorRequestContext,
        ConnectorSplitPlanningRequest,
    };

    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::ChunkSlotSchema;

    struct NotCancelled;

    impl ConnectorCancellation for NotCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            Arc::new(NotCancelled),
            1024 * 1024,
            4 * 1024 * 1024,
        )
        .expect("request context")
    }

    fn batch_budget() -> ConnectorBatchBudget {
        ConnectorBatchBudget {
            max_rows: NonZeroUsize::new(1024).expect("nonzero rows"),
            max_bytes: NonZeroUsize::new(1024 * 1024).expect("nonzero bytes"),
        }
    }

    fn config(jdbc_url: String) -> JdbcInstanceConfig {
        let chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                Field::new("id", DataType::Int64, true),
                None,
                None,
            )])
            .expect("chunk schema"),
        );
        JdbcInstanceConfig {
            scan: JdbcScanConfig {
                jdbc_url,
                jdbc_user: Some("must-not-cross-handle-boundary".to_string()),
                jdbc_passwd: Some("secret".to_string()),
                table: "orders".to_string(),
                columns: vec!["id".to_string()],
                filters: Vec::new(),
                limit: None,
                chunk_schema,
            },
        }
    }

    #[test]
    fn jdbc_instance_reads_sqlite_through_spi_without_transporting_credentials() {
        let database = tempfile::NamedTempFile::new().expect("sqlite database file");
        let connection = rusqlite::Connection::open(database.path()).expect("open sqlite");
        connection
            .execute_batch("CREATE TABLE orders (id INTEGER); INSERT INTO orders VALUES (7);")
            .expect("seed sqlite table");
        drop(connection);

        let instance = JdbcConnectorInstance::new(
            ConnectorInstanceId::parse("jdbc.test").expect("instance ID"),
            config(format!("jdbc:sqlite:{}", database.path().display())),
        );
        let table = instance.table_handle().expect("table handle");
        assert!(
            !std::str::from_utf8(table.payload())
                .expect("table payload utf8")
                .contains("secret"),
            "table handle must not contain provider credentials"
        );
        let scan = instance
            .begin_scan(
                &table,
                ConnectorBeginScanRequest {
                    projection: vec![0],
                    selector: ConnectorReadSelector::Current,
                    limit: None,
                    batch: batch_budget(),
                    context: context(),
                },
            )
            .expect("begin scan");
        let splits = instance
            .plan_splits(
                &scan.handle,
                ConnectorSplitPlanningRequest {
                    target_parallelism: NonZeroUsize::new(1).expect("parallelism"),
                    max_split_bytes: None,
                    context: context(),
                },
            )
            .expect("plan splits");
        assert_eq!(splits.len(), 1);
        assert!(
            !std::str::from_utf8(splits[0].payload())
                .expect("split payload utf8")
                .contains("secret"),
            "split payload must not contain provider credentials"
        );
        let mut reader = instance
            .open_reader(
                &splits[0],
                ConnectorOpenReaderRequest {
                    expected_schema: scan.output_schema,
                    batch: batch_budget(),
                    context: context(),
                },
            )
            .expect("open reader");
        let batch = reader
            .next_batch()
            .expect("read batch")
            .expect("expected one batch");
        assert_eq!(batch.num_rows(), 1);
        assert!(reader.next_batch().expect("read EOS").is_none());
        reader.close().expect("close reader");
    }

    #[test]
    fn jdbc_spi_source_unregisters_query_local_credentials_after_drop() {
        let registry = ConnectorRegistry::new();
        let instance_id = ConnectorInstanceId::parse("jdbc.lifecycle").expect("instance ID");
        let source = plan_jdbc_read_source(
            &registry,
            instance_id.clone(),
            config("jdbc:sqlite::memory:".to_string()),
            batch_budget(),
            context(),
            NonZeroUsize::new(1).expect("parallelism"),
        )
        .expect("plan JDBC SPI source");
        assert!(registry.connector_instance(&instance_id).is_ok());
        drop(source);
        assert!(registry.connector_instance(&instance_id).is_err());
    }
}
