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

use std::sync::{Arc, Weak};
use std::time::Instant;

use arrow::datatypes::{Field, Schema};
use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorBatchReader, ConnectorBeginScanRequest, ConnectorError, ConnectorErrorKind,
    ConnectorInstance, ConnectorInstanceDescriptor, ConnectorInstanceId,
    ConnectorListTablesRequest, ConnectorMetadata, ConnectorNamespaceRequest,
    ConnectorOpenReaderRequest, ConnectorProviderId, ConnectorRead, ConnectorScan,
    ConnectorScanHandle, ConnectorSplit, ConnectorSplitPlanningRequest, ConnectorTableHandle,
    ConnectorTableMetadata, ConnectorTableRequest,
};
use serde::{Deserialize, Serialize};

use crate::connector::scan_model::starrocks::{
    StarRocksScanSourceDescriptor, validate_starrocks_source_descriptor,
};
use crate::engine::StandaloneState;

const PROVIDER_ID: &str = "starrocks";
const INSTANCE_ID: &str = "starrocks";

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct TablePayload {
    pub(crate) database: String,
    pub(crate) table: String,
    pub(crate) db_id: i64,
    pub(crate) table_id: i64,
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct ScanPayload {
    pub(crate) table: TablePayload,
    pub(crate) source: StarRocksScanSourceDescriptor,
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct SplitPayload {
    pub(crate) tablet_id: i64,
    pub(crate) partition_id: i64,
    pub(crate) version: i64,
}

struct StarRocksTableConnectorInstance {
    instance_id: ConnectorInstanceId,
    state: Weak<StandaloneState>,
}

impl StarRocksTableConnectorInstance {
    fn state(&self) -> Result<Arc<StandaloneState>, ConnectorError> {
        self.state.upgrade().ok_or_else(|| {
            ConnectorError::new(ConnectorErrorKind::Unavailable, "standalone state dropped")
        })
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

    fn runtime(
        &self,
        table: &TablePayload,
    ) -> Result<super::catalog::StarRocksTableRuntime, ConnectorError> {
        let state = self.state()?;
        let catalog = state.starrocks_table.read().map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::Unavailable,
                format!("StarRocks table catalog read lock: {error}"),
            )
        })?;
        let runtime = catalog
            .table(&table.database, &table.table)
            .map_err(not_found)?
            .clone();
        if runtime.table.db_id != table.db_id || runtime.table.table_id != table.table_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                format!(
                    "StarRocks table identity mismatch for {}.{}: requested=({}, {}) runtime=({}, {})",
                    table.database,
                    table.table,
                    table.db_id,
                    table.table_id,
                    runtime.table.db_id,
                    runtime.table.table_id
                ),
            ));
        }
        Ok(runtime)
    }
}

impl ConnectorMetadata for StarRocksTableConnectorInstance {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }

    fn namespace_exists(&self, request: ConnectorNamespaceRequest) -> Result<bool, ConnectorError> {
        self.validate_context(&request.context)?;
        ensure_owner(&request.namespace.instance_id, &self.instance_id)?;
        let state = self.state()?;
        let catalog = state.starrocks_table.read().map_err(internal_lock)?;
        Ok(!catalog
            .list_tables_in_database(&request.namespace.namespace)
            .map_err(invalid)?
            .is_empty())
    }

    fn table_exists(&self, request: ConnectorTableRequest) -> Result<bool, ConnectorError> {
        self.validate_context(&request.context)?;
        ensure_owner(&request.table.instance_id, &self.instance_id)?;
        let state = self.state()?;
        state
            .starrocks_table
            .read()
            .map_err(internal_lock)?
            .contains_table(&request.table.namespace, &request.table.table)
            .map_err(invalid)
    }

    fn list_tables(
        &self,
        request: ConnectorListTablesRequest,
    ) -> Result<Vec<novarocks_spi::connector::ConnectorTableIdentity>, ConnectorError> {
        self.validate_context(&request.context)?;
        ensure_owner(&request.namespace.instance_id, &self.instance_id)?;
        let state = self.state()?;
        state
            .starrocks_table
            .read()
            .map_err(internal_lock)?
            .list_tables_in_database(&request.namespace.namespace)
            .map_err(invalid)?
            .into_iter()
            .map(|table| {
                Ok(novarocks_spi::connector::ConnectorTableIdentity {
                    instance_id: self.instance_id.clone(),
                    namespace: Arc::clone(&request.namespace.namespace),
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
        let state = self.state()?;
        let catalog = state.starrocks_table.read().map_err(internal_lock)?;
        let runtime = catalog
            .table(&request.table.namespace, &request.table.table)
            .map_err(not_found)?;
        let table = TablePayload {
            database: runtime.database_name.clone(),
            table: runtime.table.name.clone(),
            db_id: runtime.table.db_id,
            table_id: runtime.table.table_id,
        };
        let table_def = super::catalog::starrocks_table_def(runtime).map_err(invalid)?;
        let schema = Arc::new(Schema::new(
            table_def
                .columns
                .iter()
                .map(|column| Field::new(&column.name, column.data_type.clone(), column.nullable))
                .collect::<Vec<_>>(),
        ));
        Ok(ConnectorTableMetadata {
            identity: request.table,
            schema,
            version: Some(Bytes::copy_from_slice(
                &runtime.table.current_schema_id.to_le_bytes(),
            )),
            table: ConnectorTableHandle::try_new(
                self.instance_id.clone(),
                encode(&table, "table handle")?,
            )?,
        })
    }
}

impl ConnectorRead for StarRocksTableConnectorInstance {
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
        let table: TablePayload = decode(table.payload(), "table handle")?;
        let runtime = self.runtime(&table)?;
        let source = super::scan_adapter::source_descriptor(&runtime).map_err(invalid)?;
        validate_starrocks_source_descriptor(0, table.db_id, table.table_id, &source)
            .map_err(invalid)?;
        Ok(ConnectorScan {
            handle: ConnectorScanHandle::try_new(
                self.instance_id.clone(),
                encode(&ScanPayload { table, source }, "scan handle")?,
            )?,
            output_schema: Arc::new(Schema::empty()),
        })
    }

    fn plan_splits(
        &self,
        scan: &ConnectorScanHandle,
        request: ConnectorSplitPlanningRequest,
    ) -> Result<Vec<ConnectorSplit>, ConnectorError> {
        self.validate_context(&request.context)?;
        ensure_owner(scan.owner(), &self.instance_id)?;
        let scan: ScanPayload = decode(scan.payload(), "scan handle")?;
        let runtime = self.runtime(&scan.table)?;
        let current = super::scan_adapter::source_descriptor(&runtime).map_err(invalid)?;
        if current != scan.source {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                format!(
                    "StarRocks scan runtime metadata drift for {}.{}",
                    scan.table.database, scan.table.table
                ),
            ));
        }
        super::catalog::starrocks_scan_tablets(&runtime)
            .into_iter()
            .map(|tablet| {
                ConnectorSplit::try_new(
                    self.instance_id.clone(),
                    format!("starrocks-{}", tablet.tablet_id),
                    encode(
                        &SplitPayload {
                            tablet_id: tablet.tablet_id,
                            partition_id: tablet.partition_id,
                            version: tablet.version,
                        },
                        "split",
                    )?,
                    None,
                )
            })
            .collect()
    }

    fn open_reader(
        &self,
        _: &ConnectorSplit,
        _: ConnectorOpenReaderRequest,
    ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "standalone StarRocks planning instance does not execute readers",
        ))
    }
}

pub(crate) fn connector_instance(
    state: &Arc<StandaloneState>,
) -> Result<ConnectorInstance, ConnectorError> {
    let instance_id = ConnectorInstanceId::parse(INSTANCE_ID)?;
    let provider = Arc::new(StarRocksTableConnectorInstance {
        instance_id: instance_id.clone(),
        state: Arc::downgrade(state),
    });
    ConnectorInstance::try_new(
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse(PROVIDER_ID)?,
            instance_id,
        },
        Some(provider.clone()),
        provider,
    )
}

pub(crate) fn instance_id() -> Result<ConnectorInstanceId, ConnectorError> {
    ConnectorInstanceId::parse(INSTANCE_ID)
}

pub(crate) fn decode_scan(scan: &ConnectorScanHandle) -> Result<ScanPayload, String> {
    decode(scan.payload(), "scan handle").map_err(|error| error.to_string())
}

pub(crate) fn decode_split(split: &ConnectorSplit) -> Result<SplitPayload, String> {
    decode(split.payload(), "split").map_err(|error| error.to_string())
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
            "connector handle owner does not match its instance",
        ))
    }
}

fn encode<T: Serialize>(value: &T, kind: &str) -> Result<Bytes, ConnectorError> {
    serde_json::to_vec(value)
        .map(Bytes::from)
        .map_err(|error| invalid(format!("encode StarRocks {kind}: {error}")))
}

fn decode<T: for<'de> Deserialize<'de>>(payload: &Bytes, kind: &str) -> Result<T, ConnectorError> {
    serde_json::from_slice(payload)
        .map_err(|error| invalid(format!("decode StarRocks {kind}: {error}")))
}

fn invalid(message: impl ToString) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.to_string())
}

fn not_found(message: impl ToString) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::NotFound, message.to_string())
}

fn internal_lock<T>(message: std::sync::PoisonError<T>) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Unavailable,
        format!("StarRocks table catalog read lock: {message}"),
    )
}
