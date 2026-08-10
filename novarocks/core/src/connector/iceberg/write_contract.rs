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

//! Iceberg's provider-private v1 payloads for the connector write contract.
//!
//! These payloads deliberately describe only stable, secret-free facts.  The
//! execution binding supplies catalog clients and object-store credentials
//! locally; neither can cross a native fragment boundary.

use std::collections::{BTreeMap, HashMap};

use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use novarocks_catalog::schema::ColumnDef;
use novarocks_connector_iceberg::iceberg::spec::TableMetadata;
use parquet::basic::Compression;
use serde::{Deserialize, Serialize};

use novarocks_spi::connector::{
    CONNECTOR_WRITE_CONTRACT_VERSION, ConnectorCommittedVersion, ConnectorExecutionBindingKey,
    ConnectorStagedReport, ConnectorStagedReportSummary, ConnectorWriteReceipt,
    ConnectorWriterHandle, ConnectorWriterIdentity, ConnectorWriterTerminalState,
};

use crate::sql::planner::distributed::write::contract::SqlWriteSinkMode;

use super::commit::DeletionVector;
use super::sink_plan::{
    IcebergSinkMode, IcebergSinkObjectStoreConfig, IcebergSinkPlan, PositionDeleteDataFilePartition,
};
use novarocks_connector_iceberg::commit::EqualityDeleteColumn;
use novarocks_connector_iceberg::commit::report::{
    IcebergColumnStats, IcebergPartitionReport, IcebergWriterReport, IcebergWrittenFileReport,
    partition_path_from_struct,
};
use novarocks_connector_iceberg::delete_file::{IcebergFileContent, IcebergFileFormat};
use novarocks_connector_iceberg::row_lineage_synth::{
    ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
    ICEBERG_RESERVED_FIELD_ID_ROW_ID, ICEBERG_ROW_ID_COL,
};
use novarocks_connector_iceberg::scan_model::{
    IcebergSchemaDef, IcebergSchemaFieldDef, IcebergTableInfo,
};
use novarocks_connector_iceberg::write_codec::{
    ICEBERG_WRITE_PAYLOAD_VERSION, IcebergPositionDeleteBinding as ProviderPositionDeleteBinding,
    IcebergPositionDeletePartitionInput as ProviderPositionDeletePartitionInput,
    IcebergWriteHandleInput as ProviderWriteHandleInput,
    IcebergWriteHandleMode as ProviderWriteHandleMode, decode_write_handle, encode_write_handle,
};
use novarocks_connector_iceberg::write_descriptor::{
    IcebergPartitionDescriptor, IcebergPartitionValueDescriptor, decode_partition_descriptor,
    encode_partition_descriptor,
};

/// Provider-private facts retained until native writer registration. SQL
/// receives only the separately admitted SQL contract.
#[derive(Clone, Debug)]
pub(crate) struct IcebergWriteSinkSpec {
    pub(crate) mode: IcebergWriteSinkMode,
    pub(crate) iceberg: IcebergTableInfo,
    pub(crate) target_columns: Vec<ColumnDef>,
    pub(crate) table_location: String,
    pub(crate) data_location: String,
    pub(crate) target_partition_spec_id: i32,
    pub(crate) cloud_properties: BTreeMap<String, String>,
    pub(crate) file_format: String,
    pub(crate) compression: IcebergWriteFileCompression,
    pub(crate) position_delete_output_descriptor: Option<
        novarocks_connector_iceberg::position_delete_descriptor::PositionDeleteDescriptorInput,
    >,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IcebergWriteSinkMode {
    Data,
    RowLineageData,
    PositionDeletes,
    DeletionVectors,
    EqualityDeletes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IcebergWriteFileCompression {
    Snappy,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IcebergWriteHandleMode {
    Data,
    EqualityDeletes,
    PositionDeletes,
    DeletionVectors,
}

pub(crate) fn write_handle_mode(payload: &[u8]) -> Result<IcebergWriteHandleMode, String> {
    Ok(match decode_write_handle(payload)?.mode {
        ProviderWriteHandleMode::Data => IcebergWriteHandleMode::Data,
        ProviderWriteHandleMode::EqualityDeletes => IcebergWriteHandleMode::EqualityDeletes,
        ProviderWriteHandleMode::PositionDeletes => IcebergWriteHandleMode::PositionDeletes,
        ProviderWriteHandleMode::DeletionVectors => IcebergWriteHandleMode::DeletionVectors,
    })
}

impl IcebergWriteSinkSpec {
    pub(crate) fn set_planned_snapshot_id(
        &mut self,
        planned_snapshot_id: Option<i64>,
    ) -> Result<(), String> {
        self.iceberg.current_snapshot_id = planned_snapshot_id;
        Ok(())
    }

    pub(crate) fn sql_mode(&self) -> SqlWriteSinkMode {
        match self.mode {
            IcebergWriteSinkMode::Data => SqlWriteSinkMode::Data,
            IcebergWriteSinkMode::RowLineageData => SqlWriteSinkMode::RowLineageData,
            IcebergWriteSinkMode::PositionDeletes => SqlWriteSinkMode::PositionDeletes,
            IcebergWriteSinkMode::DeletionVectors => SqlWriteSinkMode::DeletionVectors,
            IcebergWriteSinkMode::EqualityDeletes => SqlWriteSinkMode::EqualityDeletes,
        }
    }
}

pub(crate) fn transform_to_sink_string(
    transform: &novarocks_connector_iceberg::iceberg::spec::Transform,
) -> String {
    transform.to_string()
}

pub(crate) fn iceberg_write_sink_mode(mode: SqlWriteSinkMode) -> IcebergWriteSinkMode {
    match mode {
        SqlWriteSinkMode::Data => IcebergWriteSinkMode::Data,
        SqlWriteSinkMode::RowLineageData => IcebergWriteSinkMode::RowLineageData,
        SqlWriteSinkMode::PositionDeletes => IcebergWriteSinkMode::PositionDeletes,
        SqlWriteSinkMode::DeletionVectors => IcebergWriteSinkMode::DeletionVectors,
        SqlWriteSinkMode::EqualityDeletes => IcebergWriteSinkMode::EqualityDeletes,
    }
}

/// Encode the non-sensitive facts from a legacy sink plan in deterministic,
/// compact JSON.  `serde_json::Map` is ordered by default and all map-bearing
/// report fields below use `BTreeMap`, so equal facts always produce equal
/// bytes and therefore equal SPI digests.
pub(crate) fn encode_sink_plan_handle_payload(plan: &IcebergSinkPlan) -> Result<Bytes, String> {
    encode_write_handle(&ProviderWriteHandleInput {
        mode: match plan.mode {
            IcebergSinkMode::Data => ProviderWriteHandleMode::Data,
            IcebergSinkMode::EqualityDeletes => ProviderWriteHandleMode::EqualityDeletes,
            IcebergSinkMode::PositionDeletes => ProviderWriteHandleMode::PositionDeletes,
            IcebergSinkMode::DeletionVectors => ProviderWriteHandleMode::DeletionVectors,
        },
        table_location: plan.table_location.clone(),
        data_location: plan.data_location.clone(),
        target_partition_spec_id: plan.target_partition_spec_id,
        target_snapshot_id: plan.target_snapshot_id,
        file_format: plan.file_format,
        report_file_format: plan.report_file_format.clone(),
        compression: plan.compression,
        equality_delete_columns: plan.equality_delete_columns.clone(),
        row_lineage_data: plan.row_lineage_data,
        partition_source_column_names: plan.partition_source_column_names.clone(),
        partition_column_names: plan.partition_column_names.clone(),
        transform_exprs: plan.transform_exprs.clone(),
        data_input_schema: None,
        position_delete_binding: plan.position_delete_binding.as_ref().map(|binding| {
            ProviderPositionDeleteBinding {
                output_column_names: binding.output_column_names.clone(),
                partition_source_column_names: binding.partition_source_column_names.clone(),
                partition_column_names: binding.partition_column_names.clone(),
            }
        }),
        position_delete_partitions: Vec::new(),
    })
}

/// Build the secret-free data-file writer template directly from the FE-owned
/// distributed sink specification.  Ordinary DATA and row-lineage DATA both
/// stage Iceberg data files; delete modes require their own provider adapters.
pub(crate) fn encode_data_sink_spec_handle_payload(
    spec: &IcebergWriteSinkSpec,
) -> Result<Bytes, String> {
    if !matches!(
        spec.mode,
        IcebergWriteSinkMode::Data | IcebergWriteSinkMode::RowLineageData
    ) {
        return Err("only Iceberg data-file sinks can use the data writer template".to_string());
    }
    let serialized = spec.iceberg.serialized_metadata.as_deref().ok_or_else(|| {
        "Iceberg DATA writer template requires serialized table metadata".to_string()
    })?;
    let metadata: TableMetadata = serde_json::from_str(serialized)
        .map_err(|error| format!("decode Iceberg DATA writer table metadata: {error}"))?;
    let partition_spec = metadata
        .partition_spec_by_id(spec.target_partition_spec_id)
        .ok_or_else(|| {
            format!(
                "Iceberg DATA writer template references unknown partition spec {}",
                spec.target_partition_spec_id
            )
        })?;
    let mut partition_source_column_names = Vec::with_capacity(partition_spec.fields().len());
    let mut partition_column_names = Vec::with_capacity(partition_spec.fields().len());
    let mut transform_exprs = Vec::with_capacity(partition_spec.fields().len());
    for field in partition_spec.fields() {
        let source = metadata
            .current_schema()
            .field_by_id(field.source_id)
            .ok_or_else(|| {
                format!(
                    "Iceberg DATA writer partition field {} has unknown source column {}",
                    field.name, field.source_id
                )
            })?;
        partition_source_column_names.push(source.name.clone());
        partition_column_names.push(field.name.clone());
        transform_exprs.push(field.transform.to_string());
    }
    encode_write_handle(&ProviderWriteHandleInput {
        mode: ProviderWriteHandleMode::Data,
        table_location: spec.table_location.clone(),
        data_location: spec.data_location.clone(),
        target_partition_spec_id: spec.target_partition_spec_id,
        target_snapshot_id: spec.iceberg.current_snapshot_id,
        file_format: IcebergFileFormat::Parquet,
        report_file_format: spec.file_format.to_ascii_lowercase(),
        compression: Compression::SNAPPY,
        equality_delete_columns: Vec::new(),
        row_lineage_data: spec.mode == IcebergWriteSinkMode::RowLineageData,
        partition_source_column_names,
        partition_column_names,
        transform_exprs,
        data_input_schema: Some(spec.iceberg.schema.clone()),
        position_delete_binding: None,
        position_delete_partitions: Vec::new(),
    })
}

/// Build the secret-free data writer handle for a provider-frozen rewrite
/// without reconstructing a SQL-layer sink specification.  E2 has already
/// frozen the target metadata and Arrow schema before this call; the BE still
/// receives only this bounded handle and resolves storage access locally.
pub(crate) fn encode_frozen_data_rewrite_handle_payload(
    metadata: &TableMetadata,
    target_snapshot_id: Option<i64>,
    row_lineage_data: bool,
) -> Result<Bytes, String> {
    let partition_spec = metadata.default_partition_spec();
    let mut partition_source_column_names = Vec::with_capacity(partition_spec.fields().len());
    let mut partition_column_names = Vec::with_capacity(partition_spec.fields().len());
    let mut transform_exprs = Vec::with_capacity(partition_spec.fields().len());
    for field in partition_spec.fields() {
        let source = metadata
            .current_schema()
            .field_by_id(field.source_id)
            .ok_or_else(|| {
                format!(
                    "Iceberg frozen rewrite partition field {} has unknown source column {}",
                    field.name, field.source_id
                )
            })?;
        partition_source_column_names.push(source.name.clone());
        partition_column_names.push(field.name.clone());
        transform_exprs.push(field.transform.to_string());
    }
    let data_location = metadata
        .properties()
        .get("write.data.path")
        .cloned()
        .unwrap_or_else(|| format!("{}/data", metadata.location().trim_end_matches('/')));
    let mut data_input_schema =
        novarocks_connector_iceberg::schema_facts::iceberg_schema_def(metadata.current_schema())
            .fields;
    if row_lineage_data {
        data_input_schema.extend([
            IcebergSchemaFieldDef {
                field_id: ICEBERG_RESERVED_FIELD_ID_ROW_ID,
                name: ICEBERG_ROW_ID_COL.to_string(),
                initial_default: None,
                write_default: None,
                initial_default_json: None,
                write_default_json: None,
                children: Vec::new(),
            },
            IcebergSchemaFieldDef {
                field_id: ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
                name: ICEBERG_LAST_UPDATED_SEQ_COL.to_string(),
                initial_default: None,
                write_default: None,
                initial_default_json: None,
                write_default_json: None,
                children: Vec::new(),
            },
        ]);
    }
    encode_write_handle(&ProviderWriteHandleInput {
        mode: ProviderWriteHandleMode::Data,
        table_location: metadata.location().to_string(),
        data_location,
        target_partition_spec_id: metadata.default_partition_spec_id(),
        target_snapshot_id,
        file_format: IcebergFileFormat::Parquet,
        report_file_format: "parquet".to_string(),
        compression: Compression::SNAPPY,
        equality_delete_columns: Vec::new(),
        row_lineage_data,
        partition_source_column_names,
        partition_column_names,
        transform_exprs,
        // Native connector output columns intentionally carry only generic
        // Arrow facts. Preserve the Iceberg field-ID tree in this private
        // handle so the BE can re-annotate that generic schema before writing
        // Parquet; the frozen source schema itself never crosses the wire.
        data_input_schema: Some(IcebergSchemaDef {
            fields: data_input_schema,
        }),
        position_delete_binding: None,
        position_delete_partitions: Vec::new(),
    })
}

/// Build the secret-free equality-delete handle from the FE-owned sink spec.
/// C1 intentionally admits only the existing unpartitioned equality-delete
/// path; partitioned delete semantics require a later provider adapter.
pub(crate) fn encode_equality_delete_sink_spec_handle_payload(
    spec: &IcebergWriteSinkSpec,
    columns: &[EqualityDeleteColumn],
) -> Result<Bytes, String> {
    if spec.mode != IcebergWriteSinkMode::EqualityDeletes {
        return Err("only Iceberg equality-delete sinks can use this writer template".to_string());
    }
    if columns.is_empty() {
        return Err("Iceberg equality-delete writer template requires columns".to_string());
    }
    let serialized = spec.iceberg.serialized_metadata.as_deref().ok_or_else(|| {
        "Iceberg equality-delete writer template requires serialized table metadata".to_string()
    })?;
    let metadata: TableMetadata = serde_json::from_str(serialized)
        .map_err(|error| format!("decode Iceberg equality-delete table metadata: {error}"))?;
    if !metadata.default_partition_spec().is_unpartitioned() {
        return Err(
            "Iceberg connector equality-delete writer template supports only unpartitioned tables"
                .to_string(),
        );
    }
    encode_write_handle(&ProviderWriteHandleInput {
        mode: ProviderWriteHandleMode::EqualityDeletes,
        table_location: spec.table_location.clone(),
        data_location: spec.data_location.clone(),
        target_partition_spec_id: spec.target_partition_spec_id,
        target_snapshot_id: spec.iceberg.current_snapshot_id,
        file_format: IcebergFileFormat::Parquet,
        report_file_format: spec.file_format.to_ascii_lowercase(),
        compression: Compression::SNAPPY,
        equality_delete_columns: columns.to_vec(),
        row_lineage_data: false,
        partition_source_column_names: Vec::new(),
        partition_column_names: Vec::new(),
        transform_exprs: Vec::new(),
        data_input_schema: None,
        position_delete_binding: None,
        position_delete_partitions: Vec::new(),
    })
}

/// Reconstruct the unpartitioned equality-delete facts needed by the BE-only
/// writer.  The actual Arrow types remain the generic sink input schema; the
/// opaque handle only pins their Iceberg field IDs and rejects a stale or
/// differently-shaped fragment before it can create a delete file.
pub(crate) fn equality_delete_handle_from_payload(
    payload: &[u8],
    input_schema: SchemaRef,
) -> Result<(String, i32, Vec<EqualityDeleteColumn>), String> {
    let payload = decode_write_handle(payload)?;
    if payload.mode != ProviderWriteHandleMode::EqualityDeletes {
        return Err(
            "Iceberg connector writer mode is not supported by the equality-delete execution adapter"
                .to_string(),
        );
    }
    if !payload.partition_source_column_names.is_empty()
        || !payload.partition_column_names.is_empty()
        || !payload.transform_exprs.is_empty()
    {
        return Err(
            "Iceberg connector equality-delete execution supports only unpartitioned tables"
                .to_string(),
        );
    }
    if payload.equality_delete_columns.is_empty() {
        return Err("Iceberg connector equality-delete handle has no equality columns".to_string());
    }
    if payload.equality_delete_columns.len() != input_schema.fields().len() {
        return Err(format!(
            "Iceberg equality-delete handle has {} columns but fragment input has {}",
            payload.equality_delete_columns.len(),
            input_schema.fields().len()
        ));
    }
    let columns = payload
        .equality_delete_columns
        .iter()
        .zip(input_schema.fields().iter())
        .map(|(expected, actual)| {
            if expected.name != actual.name().as_str()
                || expected.data_type != format!("{:?}", actual.data_type())
                || expected.nullable != actual.is_nullable()
            {
                return Err(format!(
                    "Iceberg equality-delete handle column `{}` does not match fragment input `{}`",
                    expected.name,
                    actual.name()
                ));
            }
            Ok(EqualityDeleteColumn {
                name: expected.name.clone(),
                field_id: expected.field_id,
                data_type: actual.data_type().clone(),
                nullable: actual.is_nullable(),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok((
        payload.data_location,
        payload.target_partition_spec_id,
        columns,
    ))
}

/// Provider-private facts used by the BE position-delete and deletion-vector
/// staging adapters.  The FE freezes this index at the target snapshot; the
/// BE never opens a catalog or infers a partition from a newer snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IcebergPositionDeleteHandle {
    pub(crate) mode: IcebergWriteHandleMode,
    pub(crate) data_location: String,
    pub(crate) report_file_format: String,
    pub(crate) compression: Compression,
    pub(crate) partitions: BTreeMap<String, IcebergPositionDeletePartition>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IcebergPositionDeletePartition {
    pub(crate) partition_path: String,
    pub(crate) null_fingerprint: String,
    pub(crate) partition_spec_id: i32,
    pub(crate) descriptor: IcebergPartitionDescriptor,
    /// Iceberg's canonical DV payload as read by the FE from the frozen target
    /// snapshot.  It is a data fact, not a credential or process-local handle.
    pub(crate) existing_deletion_vector_payload: Option<Vec<u8>>,
}

/// Encode the FE-frozen data-file partition lookup in a canonical writer
/// handle.  The caller must pass the exact target snapshot index; payload
/// bounds are enforced by `ConnectorWriterHandle`/`ConnectorWritePlan` before
/// the result reaches a BE.
pub(crate) fn encode_position_delete_sink_handle_payload(
    spec: &IcebergWriteSinkSpec,
    metadata: &TableMetadata,
    partitions: &HashMap<String, PositionDeleteDataFilePartition>,
) -> Result<Bytes, String> {
    encode_position_delete_handle_payload(spec, metadata, partitions, None)
}

/// Encode a deletion-vector writer handle.  Existing positions are read by
/// the FE against the planned snapshot and serialized as canonical Iceberg DV
/// payloads, so the BE never resolves prior delete files or table metadata.
pub(crate) fn encode_deletion_vector_sink_handle_payload(
    spec: &IcebergWriteSinkSpec,
    metadata: &TableMetadata,
    partitions: &HashMap<String, PositionDeleteDataFilePartition>,
    existing_vectors: &HashMap<String, DeletionVector>,
) -> Result<Bytes, String> {
    encode_position_delete_handle_payload(spec, metadata, partitions, Some(existing_vectors))
}

/// Encode a DV rewrite writer handle from provider-frozen partition facts.
/// Unlike ordinary row-level DELETE, the frozen rewrite source has already
/// materialized every old position into the Arrow stream, so this handle must
/// not include an `existing_deletion_vector_payload` that would merge a stale
/// second copy on the BE.
pub(crate) fn encode_frozen_deletion_vector_rewrite_handle_payload(
    metadata: &TableMetadata,
    target_snapshot_id: Option<i64>,
    partitions: &HashMap<String, PositionDeleteDataFilePartition>,
) -> Result<Bytes, String> {
    let data_location = metadata
        .properties()
        .get("write.data.path")
        .cloned()
        .unwrap_or_else(|| format!("{}/data", metadata.location().trim_end_matches('/')));
    let mut encoded_partitions = partitions
        .iter()
        .map(|(data_file_path, partition)| {
            let partition_spec = metadata
                .partition_spec_by_id(partition.partition_spec_id)
                .ok_or_else(|| {
                    format!(
                        "Iceberg frozen DV rewrite references unknown partition spec {}",
                        partition.partition_spec_id
                    )
                })?;
            let (partition_path, null_fingerprint) =
                partition_path_from_struct(&partition.partition_values, partition_spec)?;
            let descriptor = encode_partition_descriptor(
                &partition.partition_values,
                partition.partition_spec_id,
                metadata,
            )
            .map_err(|error| format!("encode frozen DV rewrite partition: {error}"))?;
            Ok(ProviderPositionDeletePartitionInput {
                data_file_path: data_file_path.clone(),
                partition_path,
                null_fingerprint,
                partition_spec_id: partition.partition_spec_id,
                descriptor,
                existing_deletion_vector_payload: None,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    encoded_partitions.sort_by(|left, right| left.data_file_path.cmp(&right.data_file_path));
    if encoded_partitions
        .windows(2)
        .any(|pair| pair[0].data_file_path == pair[1].data_file_path)
    {
        return Err("Iceberg frozen DV rewrite handle has duplicate data-file paths".to_string());
    }
    encode_write_handle(&ProviderWriteHandleInput {
        mode: ProviderWriteHandleMode::DeletionVectors,
        table_location: metadata.location().to_string(),
        data_location,
        target_partition_spec_id: metadata.default_partition_spec_id(),
        target_snapshot_id,
        file_format: IcebergFileFormat::Puffin,
        report_file_format: "puffin".to_string(),
        compression: Compression::SNAPPY,
        equality_delete_columns: Vec::new(),
        row_lineage_data: false,
        partition_source_column_names: Vec::new(),
        partition_column_names: Vec::new(),
        transform_exprs: Vec::new(),
        data_input_schema: None,
        position_delete_binding: None,
        position_delete_partitions: encoded_partitions,
    })
}

fn encode_position_delete_handle_payload(
    spec: &IcebergWriteSinkSpec,
    metadata: &TableMetadata,
    partitions: &HashMap<String, PositionDeleteDataFilePartition>,
    existing_vectors: Option<&HashMap<String, DeletionVector>>,
) -> Result<Bytes, String> {
    if !matches!(
        spec.mode,
        IcebergWriteSinkMode::PositionDeletes | IcebergWriteSinkMode::DeletionVectors
    ) {
        return Err(
            "only Iceberg position-delete or deletion-vector sinks can use this writer template"
                .to_string(),
        );
    }
    let mut encoded_partitions = partitions
        .iter()
        .map(|(data_file_path, partition)| {
            let partition_spec = metadata
                .partition_spec_by_id(partition.partition_spec_id)
                .ok_or_else(|| {
                    format!(
                        "Iceberg position-delete handle references unknown partition spec {}",
                        partition.partition_spec_id
                    )
                })?;
            let (partition_path, null_fingerprint) =
                partition_path_from_struct(&partition.partition_values, partition_spec)?;
            let descriptor = encode_partition_descriptor(
                &partition.partition_values,
                partition.partition_spec_id,
                metadata,
            )
            .map_err(|error| format!("encode Iceberg position-delete partition: {error}"))?;
            let existing_deletion_vector_payload = existing_vectors
                .and_then(|vectors| vectors.get(data_file_path))
                .map(|vector| vector.to_iceberg_payload())
                .transpose()
                .map_err(|error| format!("encode frozen Iceberg deletion vector: {error}"))?;
            Ok(ProviderPositionDeletePartitionInput {
                data_file_path: data_file_path.clone(),
                partition_path,
                null_fingerprint,
                partition_spec_id: partition.partition_spec_id,
                descriptor,
                existing_deletion_vector_payload,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    encoded_partitions.sort_by(|left, right| left.data_file_path.cmp(&right.data_file_path));
    if encoded_partitions
        .windows(2)
        .any(|pair| pair[0].data_file_path == pair[1].data_file_path)
    {
        return Err(
            "Iceberg position-delete handle contains duplicate data-file paths".to_string(),
        );
    }
    if existing_vectors.is_some() && spec.mode != IcebergWriteSinkMode::DeletionVectors {
        return Err("frozen deletion vectors require a deletion-vector sink".to_string());
    }
    encode_write_handle(&ProviderWriteHandleInput {
        mode: match spec.mode {
            IcebergWriteSinkMode::PositionDeletes => ProviderWriteHandleMode::PositionDeletes,
            IcebergWriteSinkMode::DeletionVectors => ProviderWriteHandleMode::DeletionVectors,
            _ => unreachable!("validated position-delete writer mode"),
        },
        table_location: spec.table_location.clone(),
        data_location: spec.data_location.clone(),
        target_partition_spec_id: spec.target_partition_spec_id,
        target_snapshot_id: spec.iceberg.current_snapshot_id,
        file_format: IcebergFileFormat::Parquet,
        report_file_format: spec.file_format.to_ascii_lowercase(),
        compression: Compression::SNAPPY,
        equality_delete_columns: Vec::new(),
        row_lineage_data: false,
        partition_source_column_names: Vec::new(),
        partition_column_names: Vec::new(),
        transform_exprs: Vec::new(),
        data_input_schema: None,
        position_delete_binding: None,
        position_delete_partitions: encoded_partitions,
    })
}

pub(crate) fn position_delete_handle_from_payload(
    payload: &[u8],
    input_schema: &arrow::datatypes::SchemaRef,
) -> Result<IcebergPositionDeleteHandle, String> {
    let payload = decode_write_handle(payload)?;
    if !matches!(
        payload.mode,
        ProviderWriteHandleMode::PositionDeletes | ProviderWriteHandleMode::DeletionVectors
    ) {
        return Err(
            "Iceberg connector writer mode is not supported by the position-delete execution adapter"
                .to_string(),
        );
    }
    if input_schema.fields().len() < 2
        || input_schema.fields()[0].name()
            != crate::connector::iceberg::catalog::backend::ICEBERG_ROW_IDENTITY_FILE_COLUMN
        || input_schema.fields()[0].data_type() != &arrow::datatypes::DataType::Utf8
        || input_schema.fields()[1].name()
            != crate::connector::iceberg::catalog::backend::ICEBERG_ROW_IDENTITY_POS_COLUMN
        || input_schema.fields()[1].data_type() != &arrow::datatypes::DataType::Int64
    {
        return Err(
            "Iceberg position-delete writer requires (_file UTF8, _pos INT64) as its first two input columns"
                .to_string(),
        );
    }
    let partitions = payload
        .position_delete_partitions
        .into_iter()
        .map(|(path, partition)| {
            (
                path,
                IcebergPositionDeletePartition {
                    partition_path: partition.partition_path,
                    null_fingerprint: partition.null_fingerprint,
                    partition_spec_id: partition.partition_spec_id,
                    descriptor: partition.descriptor,
                    existing_deletion_vector_payload: partition.existing_deletion_vector_payload,
                },
            )
        })
        .collect();
    Ok(IcebergPositionDeleteHandle {
        mode: match payload.mode {
            ProviderWriteHandleMode::PositionDeletes => IcebergWriteHandleMode::PositionDeletes,
            ProviderWriteHandleMode::DeletionVectors => IcebergWriteHandleMode::DeletionVectors,
            _ => unreachable!("validated position-delete writer mode"),
        },
        data_location: payload.data_location,
        report_file_format: payload.report_file_format,
        compression: payload.compression,
        partitions,
    })
}

pub(crate) fn writer_handle_from_sink_plan(
    owner: ConnectorExecutionBindingKey,
    writer: ConnectorWriterIdentity,
    plan: &IcebergSinkPlan,
) -> Result<ConnectorWriterHandle, String> {
    let payload = encode_sink_plan_handle_payload(plan)?;
    ConnectorWriterHandle::try_new(owner, writer, ICEBERG_WRITE_PAYLOAD_VERSION, payload)
        .map_err(|error| format!("build Iceberg connector writer handle failed: {error}"))
}
