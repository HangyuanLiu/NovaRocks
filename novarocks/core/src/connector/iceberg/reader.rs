// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to You under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the License
// at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Provider-owned physical Iceberg reader.
//!
//! `novarocks-fs` owns physical format decoding and returns physical row
//! coordinates.  This module owns the Iceberg field-ID output contract and is
//! intentionally the only place an Iceberg connector reader turns a physical
//! batch into a provider batch.

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, ArrayRef, new_null_array};
use arrow::compute::cast;
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use novarocks_fs::{
    FileBatchReader, FileCancellation, FileError, FileErrorKind, FileFormat, FileIdentity,
    FileMetricsSnapshot, FileProjection, FileReadBudget, FileReadRange, FileReadRequest,
    FsAccessResolver, ObjectStoreConfig, PhysicalPruning, open_file_reader,
};
use novarocks_spi::connector::{
    ConnectorBatchReader, ConnectorError, ConnectorErrorKind, ConnectorOpenReaderRequest,
    ConnectorReaderMetricsSnapshot,
};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use super::scan_model::IcebergDataFileInfo;

pub(crate) struct IcebergBatchReader {
    reader: Box<dyn FileBatchReader>,
    expected_schema: SchemaRef,
    context: novarocks_spi::connector::ConnectorRequestContext,
    cancellation: FileCancellation,
    closed: bool,
}

impl IcebergBatchReader {
    pub(crate) fn try_new(
        file: &IcebergDataFileInfo,
        object_store_config: Option<&ObjectStoreConfig>,
        request: ConnectorOpenReaderRequest,
    ) -> Result<Self, ConnectorError> {
        validate_context(&request.context)?;
        let file_size = u64::try_from(file.size).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                format!("Iceberg data file {} has a negative size", file.path),
            )
        })?;
        let access = FsAccessResolver::new()
            .resolve_location(&file.path, object_store_config)
            .map_err(map_file_error)?;
        let bound_file = access
            .bind_location(&file.path, FileIdentity::new(&file.path, file_size, None))
            .map_err(map_file_error)?;
        let cancellation = FileCancellation::new();
        let context = crate::connector::file_execution::foundation_read_context(
            cancellation.clone(),
            Some(request.context.deadline()),
        )
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::Internal, error))?;
        let reader = open_file_reader(FileReadRequest {
            file: bound_file,
            format: physical_format(&file.path)?,
            range: FileReadRange::WholeFile,
            projection: FileProjection::All,
            budget: FileReadBudget {
                max_rows: request.batch.max_rows,
                max_bytes: request.batch.max_bytes,
            },
            predicates: Vec::new(),
            pruning: PhysicalPruning::default(),
            cache: None,
            context,
        })
        .map_err(map_file_error)?;
        Ok(Self {
            reader,
            expected_schema: request.expected_schema,
            context: request.context,
            cancellation,
            closed: false,
        })
    }

    fn validate_context(&self) -> Result<(), ConnectorError> {
        if let Err(error) = validate_context(&self.context) {
            self.cancellation.cancel();
            return Err(error);
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
        let next = self.reader.next_batch().map_err(map_file_error)?;
        self.validate_context()?;
        match next {
            Some(file_batch) => align_batch_to_schema(&self.expected_schema, file_batch.batch)
                .map(Some)
                .map_err(|error| ConnectorError::new(ConnectorErrorKind::CorruptData, error)),
            None => {
                self.close()?;
                Ok(None)
            }
        }
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        self.cancellation.cancel();
        self.reader.close().map_err(map_file_error)
    }

    fn metrics_snapshot(&self) -> ConnectorReaderMetricsSnapshot {
        connector_metrics(self.reader.metrics_snapshot())
    }
}

impl Drop for IcebergBatchReader {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

fn physical_format(path: &str) -> Result<FileFormat, ConnectorError> {
    let path = path.split('?').next().unwrap_or(path);
    if path.to_ascii_lowercase().ends_with(".orc") {
        return Ok(FileFormat::Orc);
    }
    if path.to_ascii_lowercase().ends_with(".parquet")
        || path.to_ascii_lowercase().ends_with(".parq")
    {
        return Ok(FileFormat::Parquet);
    }
    Err(ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        format!("Iceberg data file format is not declared or supported: {path}"),
    ))
}

fn validate_context(
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

fn map_file_error(error: FileError) -> ConnectorError {
    let kind = match error.kind() {
        FileErrorKind::Invalid => ConnectorErrorKind::InvalidRequest,
        FileErrorKind::Unsupported => ConnectorErrorKind::Unsupported,
        FileErrorKind::NotFound => ConnectorErrorKind::NotFound,
        FileErrorKind::Permission => ConnectorErrorKind::PermissionDenied,
        FileErrorKind::Corrupt => ConnectorErrorKind::CorruptData,
        FileErrorKind::ResourceExhausted => ConnectorErrorKind::ResourceExhausted,
        FileErrorKind::Transient => ConnectorErrorKind::Unavailable,
        FileErrorKind::DeadlineExceeded => ConnectorErrorKind::DeadlineExceeded,
        FileErrorKind::Cancelled => ConnectorErrorKind::Cancelled,
        FileErrorKind::Internal => ConnectorErrorKind::Internal,
    };
    ConnectorError::new(kind, error.to_string())
}

fn connector_metrics(metrics: FileMetricsSnapshot) -> ConnectorReaderMetricsSnapshot {
    ConnectorReaderMetricsSnapshot {
        bytes_read: metrics.bytes_read,
        read_requests: metrics.read_requests,
        rows_decoded: metrics.rows_decoded,
        batches_delivered: metrics.batches_delivered,
        cache_hits: metrics.cache_hits,
        cache_misses: metrics.cache_misses,
        io_time_ns: metrics.io_time_ns,
        decode_time_ns: metrics.decode_time_ns,
        row_groups_read: metrics.row_groups_read,
        row_groups_pruned: metrics.row_groups_pruned,
        delayed_materialization_ranges: metrics.delayed_materialization_ranges,
    }
}

fn parse_field_id(field: &Field) -> Result<Option<i32>, String> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .map(|value| {
            value.parse::<i32>().map_err(|error| {
                format!(
                    "invalid Iceberg field ID metadata for column {}: {error}",
                    field.name()
                )
            })
        })
        .transpose()
}

fn source_index_for_target(
    source_fields: &[arrow::datatypes::FieldRef],
    target: &Field,
) -> Result<Option<usize>, String> {
    let target_id = parse_field_id(target)?;
    if let Some(target_id) = target_id {
        let mut any_source_id = false;
        for (index, source) in source_fields.iter().enumerate() {
            let source_id = parse_field_id(source.as_ref())?;
            any_source_id |= source_id.is_some();
            if source_id == Some(target_id) {
                return Ok(Some(index));
            }
        }
        if any_source_id {
            return Ok(None);
        }
    }
    Ok(source_fields
        .iter()
        .position(|source| source.name() == target.name()))
}

fn align_batch_to_schema(expected: &SchemaRef, batch: RecordBatch) -> Result<RecordBatch, String> {
    let source_schema = batch.schema();
    let mut fields = Vec::with_capacity(expected.fields().len());
    let mut columns = Vec::with_capacity(expected.fields().len());
    for target in expected.fields() {
        let column: ArrayRef =
            match source_index_for_target(source_schema.fields(), target.as_ref())? {
                Some(index) => {
                    let source = batch.column(index).clone();
                    if source.data_type() == target.data_type() {
                        source
                    } else {
                        cast(source.as_ref(), target.data_type()).map_err(|error| {
                            format!(
                                "Iceberg field {} cannot cast from {:?} to {:?}: {error}",
                                target.name(),
                                source.data_type(),
                                target.data_type()
                            )
                        })?
                    }
                }
                None if target.is_nullable() => {
                    new_null_array(target.data_type(), batch.num_rows())
                }
                None => {
                    return Err(format!(
                        "Iceberg data file is missing required field {}",
                        target.name()
                    ));
                }
            };
        let field = if column.null_count() > 0 && !target.is_nullable() {
            target.as_ref().clone().with_nullable(true)
        } else {
            target.as_ref().clone()
        };
        fields.push(field);
        columns.push(column);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field};

    use super::*;

    fn field_with_id(name: &str, field_id: i32, nullable: bool) -> Field {
        Field::new(name, DataType::Int32, nullable).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            field_id.to_string(),
        )]))
    }

    #[test]
    fn aligns_iceberg_output_by_field_id_and_fills_missing_nullable_field() {
        let source = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                field_with_id("renamed_second", 2, false),
                field_with_id("renamed_first", 1, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![20])) as ArrayRef,
                Arc::new(Int32Array::from(vec![10])) as ArrayRef,
            ],
        )
        .expect("source batch");
        let expected = Arc::new(Schema::new(vec![
            field_with_id("first", 1, false),
            field_with_id("second", 2, false),
            field_with_id("introduced_nullable", 3, true),
        ]));

        let aligned = align_batch_to_schema(&expected, source).expect("field-ID alignment");
        assert_eq!(aligned.schema().field(0).name(), "first");
        assert_eq!(aligned.schema().field(1).name(), "second");
        assert_eq!(
            aligned
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            10
        );
        assert_eq!(
            aligned
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            20
        );
        assert_eq!(aligned.column(2).null_count(), 1);
    }
}
