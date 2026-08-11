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

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use arrow::datatypes::{DataType, SchemaRef};
use novarocks_spi::connector::{
    ConnectorBatchBudget, ConnectorBatchReader, ConnectorCancellation, ConnectorError,
    ConnectorErrorKind, ConnectorExecutionBinding, ConnectorExecutionBindingKey,
    ConnectorInstanceId, ConnectorInstanceIncarnation, ConnectorOpenReaderRequest,
    ConnectorPrepareSplitRequest, ConnectorPreparedScanUnit, ConnectorPreparedScanUnitDescriptor,
    ConnectorPreparedScanUnitSet, ConnectorProviderId, ConnectorReadExecution,
    ConnectorRequestContext, ConnectorScanUnitDomainFacts, ConnectorScanUnitFactsMissingReason,
    ConnectorSplit, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
};

use crate::connector::runtime::ConnectorReadScanSource;
use crate::runtime::query_context::{QueryId, query_context_manager};
use novarocks_execution::exec::chunk::{ChunkSchema, ChunkSlotSchema};
use novarocks_execution::exec::node::scan::ScanSource;
use novarocks_execution::runtime::query_options::{QueryOptions, query_expire_durations};
use novarocks_types::SlotId;

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum IcebergMetadataTableType {
    Files,
    Manifests,
    LogicalIcebergMetadata,
    Snapshots,
    History,
    Refs,
    Partitions,
}

impl IcebergMetadataTableType {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_uppercase().as_str() {
            "FILES" => Ok(Self::Files),
            "MANIFESTS" => Ok(Self::Manifests),
            "LOGICAL_ICEBERG_METADATA" => Ok(Self::LogicalIcebergMetadata),
            "SNAPSHOTS" => Ok(Self::Snapshots),
            "HISTORY" => Ok(Self::History),
            "REFS" => Ok(Self::Refs),
            "PARTITIONS" => Ok(Self::Partitions),
            "ENTRIES" => Ok(Self::LogicalIcebergMetadata),
            other => Err(format!("unsupported iceberg metadata table type: {other}")),
        }
    }

    // Retained for diagnostics and unit-test assertions; the production reject
    // path that previously consumed it was removed once all metadata flavors
    // gained native builders.
    #[allow(dead_code)]
    fn as_uppercase_str(&self) -> &'static str {
        match self {
            Self::Files => "FILES",
            Self::Manifests => "MANIFESTS",
            Self::LogicalIcebergMetadata => "LOGICAL_ICEBERG_METADATA",
            Self::Snapshots => "SNAPSHOTS",
            Self::History => "HISTORY",
            Self::Refs => "REFS",
            Self::Partitions => "PARTITIONS",
        }
    }
}

#[derive(Clone, Debug)]
pub struct IcebergMetadataOutputColumn {
    pub name: String,
    pub slot_id: SlotId,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Clone, Debug)]
pub struct IcebergMetadataScanRange {
    pub path: String,
    pub serialized_split: String,
}

#[derive(Clone, Debug)]
pub struct IcebergMetadataScanConfig {
    pub metadata_table_type: IcebergMetadataTableType,
    pub serialized_table: String,
    pub serialized_predicate: String,
    pub load_column_stats: bool,
    pub ranges: Vec<IcebergMetadataScanRange>,
    pub batch_size: usize,
    pub output_columns: Vec<IcebergMetadataOutputColumn>,
    pub profile_label: Option<String>,
}

pub(crate) fn provider_metadata_table_type(
    value: IcebergMetadataTableType,
) -> novarocks_connector_iceberg::metadata_batch_reader::MetadataTableType {
    use novarocks_connector_iceberg::metadata_batch_reader::MetadataTableType;

    match value {
        IcebergMetadataTableType::Files => MetadataTableType::Files,
        IcebergMetadataTableType::Manifests => MetadataTableType::Manifests,
        IcebergMetadataTableType::LogicalIcebergMetadata => {
            MetadataTableType::LogicalIcebergMetadata
        }
        IcebergMetadataTableType::Snapshots => MetadataTableType::Snapshots,
        IcebergMetadataTableType::History => MetadataTableType::History,
        IcebergMetadataTableType::Refs => MetadataTableType::Refs,
        IcebergMetadataTableType::Partitions => MetadataTableType::Partitions,
    }
}

fn provider_metadata_output_columns(
    columns: &[IcebergMetadataOutputColumn],
) -> Vec<novarocks_connector_iceberg::metadata_batch_reader::MetadataOutputColumn> {
    columns
        .iter()
        .map(
            |column| novarocks_connector_iceberg::metadata_batch_reader::MetadataOutputColumn {
                name: column.name.clone(),
                data_type: column.data_type.clone(),
                nullable: column.nullable,
            },
        )
        .collect()
}

fn metadata_output_schema(config: &IcebergMetadataScanConfig) -> Result<SchemaRef, ConnectorError> {
    novarocks_connector_iceberg::metadata_batch_reader::metadata_output_schema(
        &provider_metadata_output_columns(&config.output_columns),
    )
    .map_err(|error| ConnectorError::new(ConnectorErrorKind::InvalidRequest, error))
}

const ICEBERG_METADATA_SPI_PROVIDER_ID: &str = "iceberg-metadata";

struct IcebergMetadataConnectorInstance {
    instance_id: ConnectorInstanceId,
    config: IcebergMetadataScanConfig,
    ranges: Mutex<Vec<IcebergMetadataScanRange>>,
}

impl IcebergMetadataConnectorInstance {
    fn new(instance_id: ConnectorInstanceId, config: IcebergMetadataScanConfig) -> Self {
        Self {
            instance_id,
            ranges: Mutex::new(config.ranges.clone()),
            config,
        }
    }

    fn range_count(&self) -> Result<usize, ConnectorError> {
        self.ranges
            .lock()
            .map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "Iceberg metadata range lock poisoned",
                )
            })
            .map(|ranges| ranges.len())
    }

    fn split_for_index(&self, index: usize) -> Result<ConnectorSplit, ConnectorError> {
        self.ranges
            .lock()
            .map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "Iceberg metadata range lock poisoned",
                )
            })?
            .get(index)
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "Iceberg metadata split index is out of bounds",
                )
            })?;
        ConnectorSplit::try_new(
            self.instance_id.clone(),
            format!("iceberg-metadata-{index}"),
            bytes::Bytes::copy_from_slice(&(index as u64).to_le_bytes()),
            None,
        )
    }

    fn range_for_split(
        &self,
        split: &ConnectorSplit,
    ) -> Result<IcebergMetadataScanRange, ConnectorError> {
        self.range_for_payload(split.owner(), split.payload())
    }

    fn range_for_payload(
        &self,
        owner: &ConnectorInstanceId,
        payload: &[u8],
    ) -> Result<IcebergMetadataScanRange, ConnectorError> {
        if owner != &self.instance_id || payload.len() != 8 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "invalid Iceberg metadata split payload",
            ));
        }
        let bytes: [u8; 8] = payload.try_into().expect("payload length checked");
        let index = usize::try_from(u64::from_le_bytes(bytes)).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg metadata split index overflows usize",
            )
        })?;
        self.ranges
            .lock()
            .map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "Iceberg metadata range lock poisoned",
                )
            })?
            .get(index)
            .cloned()
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "Iceberg metadata split index is out of bounds",
                )
            })
    }

    fn execution_binding(self: Arc<Self>) -> Result<ConnectorExecutionBinding, ConnectorError> {
        let key = ConnectorExecutionBindingKey {
            instance_id: self.instance_id.clone(),
            incarnation: ConnectorInstanceIncarnation::new(),
        };
        ConnectorExecutionBinding::try_new(
            ConnectorProviderId::parse(ICEBERG_METADATA_SPI_PROVIDER_ID)?,
            key.clone(),
            Arc::new(IcebergMetadataExecution { key, reader: self }),
        )
    }
}

struct IcebergMetadataExecution {
    key: ConnectorExecutionBindingKey,
    reader: Arc<IcebergMetadataConnectorInstance>,
}

impl ConnectorReadExecution for IcebergMetadataExecution {
    fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.key
    }

    fn prepare_split(
        &self,
        split: &ConnectorSplit,
        request: ConnectorPrepareSplitRequest,
    ) -> Result<ConnectorPreparedScanUnitSet, ConnectorError> {
        request.check_active()?;
        let _ = self.reader.range_for_split(split)?;
        ConnectorPreparedScanUnitSet::try_new(
            self.key.clone(),
            split,
            bytes::Bytes::new(),
            vec![ConnectorPreparedScanUnitDescriptor::try_new(
                split.payload().clone(),
                None,
                ConnectorScanUnitDomainFacts::missing(
                    ConnectorScanUnitFactsMissingReason::ProviderUnsupported,
                ),
            )?],
            &request,
        )
    }

    fn open_unit_reader(
        &self,
        unit: &ConnectorPreparedScanUnit,
        request: ConnectorOpenReaderRequest,
    ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
        if unit.binding_key() != &self.key {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg metadata prepared unit belongs to another binding",
            ));
        }
        let _ = self
            .reader
            .range_for_payload(&self.key.instance_id, unit.payload())?;
        let config = &self.reader.config;
        open_metadata_connector_reader(
            config.metadata_table_type.clone(),
            config.serialized_table.clone(),
            config.serialized_predicate.clone(),
            request.expected_schema,
            request.batch,
            request.context,
        )
    }
}

/// Open one provider-owned metadata reader under the ordinary Iceberg
/// execution binding.  The generic carrier supplies the expected Arrow
/// schema; the opaque split supplies only metadata facts, so Core never has
/// to reconstruct a metadata-table decoder or a second execution binding.
pub(crate) fn open_metadata_connector_reader(
    metadata_table_type: IcebergMetadataTableType,
    serialized_table: String,
    serialized_payload: String,
    expected_schema: SchemaRef,
    batch: ConnectorBatchBudget,
    context: ConnectorRequestContext,
) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
    novarocks_connector_iceberg::metadata_batch_reader::open_metadata_connector_reader(
        provider_metadata_table_type(metadata_table_type),
        serialized_table,
        serialized_payload,
        expected_schema,
        batch,
        context,
    )
}

struct IcebergMetadataQueryCancellation {
    query_id: Option<QueryId>,
}

impl ConnectorCancellation for IcebergMetadataQueryCancellation {
    fn is_cancelled(&self) -> bool {
        self.query_id
            .is_some_and(|query_id| query_context_manager().is_query_canceled(query_id))
    }
}

pub(crate) fn plan_iceberg_metadata_read_source(
    instance_id: ConnectorInstanceId,
    config: IcebergMetadataScanConfig,
    batch: ConnectorBatchBudget,
    context: ConnectorRequestContext,
) -> Result<Arc<dyn ScanSource>, ConnectorError> {
    let output_schema = metadata_output_schema(&config)?;
    let provider = Arc::new(IcebergMetadataConnectorInstance::new(instance_id, config));
    let splits = (0..provider.range_count()?)
        .map(|index| provider.split_for_index(index))
        .collect::<Result<Vec<_>, _>>()?;
    let chunk_schema = Arc::new(
        ChunkSchema::try_new(
            provider
                .config
                .output_columns
                .iter()
                .zip(output_schema.fields())
                .map(|(column, field)| {
                    ChunkSlotSchema::new_with_field(
                        column.slot_id,
                        field.as_ref().clone(),
                        None,
                        None,
                    )
                })
                .collect(),
        )
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::InvalidRequest, error))?,
    );
    let binding = Arc::new(Arc::clone(&provider).execution_binding()?);
    Ok(Arc::new(
        ConnectorReadScanSource::new_execution(
            binding,
            splits,
            ConnectorOpenReaderRequest {
                expected_schema: output_schema,
                batch,
                context,
            },
            chunk_schema,
        )
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::InvalidRequest, error))?,
    ))
}

fn metadata_budget_and_context(
    query_id: Option<QueryId>,
    query_options: &QueryOptions,
) -> Result<(ConnectorBatchBudget, ConnectorRequestContext), ConnectorError> {
    metadata_budget_and_context_with_cancellation(
        query_options,
        Arc::new(IcebergMetadataQueryCancellation { query_id }),
    )
}

fn metadata_budget_and_context_with_cancellation(
    query_options: &QueryOptions,
    cancellation: Arc<dyn ConnectorCancellation>,
) -> Result<(ConnectorBatchBudget, ConnectorRequestContext), ConnectorError> {
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
        cancellation,
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )?;
    Ok((batch, context))
}

pub fn plan_native_iceberg_metadata_read_source(
    query_id: Option<QueryId>,
    node_id: i32,
    config: IcebergMetadataScanConfig,
    query_options: &QueryOptions,
) -> Result<Arc<dyn ScanSource>, ConnectorError> {
    plan_native_iceberg_metadata_read_source_with_cancellation(
        query_id,
        node_id,
        config,
        query_options,
        Arc::new(IcebergMetadataQueryCancellation { query_id }),
    )
}

/// Build a native metadata scan source using Backend-supplied cancellation.
/// The decoder retains no route to the query-context manager.
pub fn plan_native_iceberg_metadata_read_source_with_cancellation(
    query_id: Option<QueryId>,
    node_id: i32,
    config: IcebergMetadataScanConfig,
    query_options: &QueryOptions,
    cancellation: Arc<dyn ConnectorCancellation>,
) -> Result<Arc<dyn ScanSource>, ConnectorError> {
    let (batch, context) =
        metadata_budget_and_context_with_cancellation(query_options, cancellation)?;
    let query_label = query_id
        .map(|query_id| query_id.to_string())
        .unwrap_or_else(|| "unidentified".to_string());
    plan_iceberg_metadata_read_source(
        ConnectorInstanceId::parse(&format!("iceberg.metadata.native.{query_label}.{node_id}"))?,
        config,
        batch,
        context,
    )
}

#[cfg(test)]
mod tests {
    use super::IcebergMetadataTableType;

    #[test]
    fn metadata_table_type_accepts_entries_alias() {
        assert_eq!(
            IcebergMetadataTableType::parse("entries").unwrap(),
            IcebergMetadataTableType::LogicalIcebergMetadata
        );
    }
}
