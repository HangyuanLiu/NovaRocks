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

//! Provider reader for a frozen Iceberg Puffin deletion-vector rewrite group.

use std::collections::VecDeque;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_fs::FileCancellation;
use novarocks_spi::connector::{
    ConnectorBatchReader, ConnectorError, ConnectorErrorKind, ConnectorOpenReaderRequest,
    ConnectorReaderMetricsSnapshot,
};
use serde::{Deserialize, Serialize};

use crate::access_binding::IcebergReadBinding;
use crate::delete_file::{IcebergDeleteFileSpec, IcebergFileContent, IcebergFileFormat};
use crate::position_delete::load_position_deletes_with_context;
use crate::scan_model::{
    IcebergDataFileInfo, IcebergDeleteFileContent, IcebergDeleteFileFormat, IcebergDeleteFileInfo,
};

pub const ICEBERG_REWRITE_POSITION_SPLIT_V1: u16 = 1;

/// Provider-private maintenance split.  Generic carriers transport this
/// opaque payload without learning Puffin metadata or deletion-vector rows.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IcebergRewritePositionSplitPayloadV1 {
    pub version: u16,
    pub selected_delete_files: Vec<IcebergDeleteFileInfo>,
}

const POSITION_BATCH_ROWS: usize = 64 * 1024;

pub struct IcebergRewritePositionBatchReader {
    request: ConnectorOpenReaderRequest,
    batches: VecDeque<RecordBatch>,
    metrics: ConnectorReaderMetricsSnapshot,
    closed: bool,
}

impl IcebergRewritePositionBatchReader {
    pub fn try_new(
        data_file: IcebergDataFileInfo,
        payload: IcebergRewritePositionSplitPayloadV1,
        binding: IcebergReadBinding,
        request: ConnectorOpenReaderRequest,
    ) -> Result<Self, ConnectorError> {
        let schema = rewrite_position_schema();
        if request.expected_schema.as_ref() != schema.as_ref() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg rewrite-position reader schema does not match the writer contract",
            ));
        }
        if request.context.cancellation().is_cancelled() {
            return Err(cancelled());
        }
        if std::time::Instant::now() >= request.context.deadline() {
            return Err(deadline());
        }
        let specs = selected_delete_specs(&data_file, &payload)?;
        let access =
            binding.resolve_access_for_locations(specs.iter().map(|spec| spec.path.as_str()))?;
        let context =
            binding.file_read_context(FileCancellation::new(), request.context.deadline())?;
        let read_bytes = specs.iter().try_fold(0_u64, |total, spec| {
            let size = spec.content_size_in_bytes.unwrap_or_default();
            let size = u64::try_from(size)
                .map_err(|_| corrupt("Iceberg rewrite-position Puffin content size is negative"))?;
            total.checked_add(size).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "Iceberg rewrite-position byte count overflows u64",
                )
            })
        })?;
        let positions =
            load_position_deletes_with_context(&specs, &data_file.path, &access, &context)
                .map_err(corrupt)?;
        let mut values = positions.iter().collect::<Vec<_>>();
        if values.iter().any(|position| *position > i64::MAX as u64) {
            return Err(corrupt(
                "Iceberg rewrite-position contains a position outside i64 range",
            ));
        }
        let rows_decoded = values.len() as u64;
        let mut batches = VecDeque::new();
        for chunk in values.chunks(POSITION_BATCH_ROWS) {
            let files = StringArray::from(vec![data_file.path.clone(); chunk.len()]);
            let positions = Int64Array::from(
                chunk
                    .iter()
                    .map(|position| *position as i64)
                    .collect::<Vec<_>>(),
            );
            batches.push_back(
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        std::sync::Arc::new(files) as ArrayRef,
                        std::sync::Arc::new(positions) as ArrayRef,
                    ],
                )
                .map_err(|error| corrupt(error.to_string()))?,
            );
        }
        values.clear();
        Ok(Self {
            request,
            batches,
            metrics: ConnectorReaderMetricsSnapshot {
                bytes_read: read_bytes,
                read_requests: specs.len() as u64,
                rows_decoded,
                ..Default::default()
            },
            closed: false,
        })
    }
}

impl ConnectorBatchReader for IcebergRewritePositionBatchReader {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        if self.closed {
            return Ok(None);
        }
        if self.request.context.cancellation().is_cancelled() {
            return Err(cancelled());
        }
        if std::time::Instant::now() >= self.request.context.deadline() {
            return Err(deadline());
        }
        let batch = self.batches.pop_front();
        if batch.is_some() {
            self.metrics.batches_delivered = self.metrics.batches_delivered.saturating_add(1);
        } else {
            self.closed = true;
        }
        Ok(batch)
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        self.closed = true;
        self.batches.clear();
        Ok(())
    }

    fn metrics_snapshot(&self) -> ConnectorReaderMetricsSnapshot {
        self.metrics
    }
}

impl Drop for IcebergRewritePositionBatchReader {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

fn selected_delete_specs(
    data_file: &IcebergDataFileInfo,
    payload: &IcebergRewritePositionSplitPayloadV1,
) -> Result<Vec<IcebergDeleteFileSpec>, ConnectorError> {
    if payload.selected_delete_files.is_empty() {
        return Err(corrupt(
            "Iceberg rewrite-position split has no selected Puffin files",
        ));
    }
    payload
        .selected_delete_files
        .iter()
        .map(|delete| {
            if !data_file
                .delete_files
                .iter()
                .any(|candidate| candidate == delete)
                || !matches!(delete.file_content, IcebergDeleteFileContent::Position)
                || !matches!(delete.file_format, IcebergDeleteFileFormat::Puffin)
            {
                return Err(corrupt(
                    "Iceberg rewrite-position split selects a foreign or non-Puffin delete file",
                ));
            }
            let length =
                delete.length.map(u64::try_from).transpose().map_err(|_| {
                    corrupt("Iceberg rewrite-position Puffin file has a negative size")
                })?;
            let offset = delete.content_offset.ok_or_else(|| {
                corrupt("Iceberg rewrite-position Puffin file is missing content_offset")
            })?;
            let content_size_in_bytes = delete.content_size_in_bytes.ok_or_else(|| {
                corrupt("Iceberg rewrite-position Puffin file is missing content_size_in_bytes")
            })?;
            if content_size_in_bytes < 0 {
                return Err(corrupt(
                    "Iceberg rewrite-position Puffin file has a negative content size",
                ));
            }
            Ok(IcebergDeleteFileSpec {
                path: delete.path.clone(),
                file_format: IcebergFileFormat::Puffin,
                file_content: IcebergFileContent::PositionDeletes,
                length,
                content_offset: Some(offset),
                content_size_in_bytes: Some(content_size_in_bytes),
            })
        })
        .collect()
}

fn rewrite_position_schema() -> std::sync::Arc<Schema> {
    std::sync::Arc::new(Schema::new(vec![
        Field::new("_file", DataType::Utf8, false),
        Field::new("_pos", DataType::Int64, false),
    ]))
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message.into())
}

fn cancelled() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Cancelled,
        "connector request was cancelled",
    )
}

fn deadline() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::DeadlineExceeded,
        "connector request deadline elapsed",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn puffin(path: &str) -> IcebergDeleteFileInfo {
        IcebergDeleteFileInfo {
            path: path.to_string(),
            file_format: IcebergDeleteFileFormat::Puffin,
            file_content: IcebergDeleteFileContent::Position,
            length: Some(1024),
            content_offset: Some(8),
            content_size_in_bytes: Some(32),
            sequence_number: Some(7),
            partition_spec_id: Some(0),
            partition_key: None,
            equality_column_names: Vec::new(),
            equality_field_ids: Vec::new(),
        }
    }

    #[test]
    fn selected_specs_reject_a_delete_file_outside_the_frozen_data_file() {
        let selected = puffin("s3://warehouse/delete-1.puffin");
        let mut file = IcebergDataFileInfo::for_test("s3://warehouse/data-1.parquet", 16, 2);
        file.delete_files.push(selected.clone());
        let payload = IcebergRewritePositionSplitPayloadV1 {
            version: ICEBERG_REWRITE_POSITION_SPLIT_V1,
            selected_delete_files: vec![selected],
        };
        let specs = selected_delete_specs(&file, &payload).expect("frozen Puffin reference");
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].path, "s3://warehouse/delete-1.puffin");

        let foreign = IcebergRewritePositionSplitPayloadV1 {
            version: ICEBERG_REWRITE_POSITION_SPLIT_V1,
            selected_delete_files: vec![puffin("s3://warehouse/delete-foreign.puffin")],
        };
        let error = selected_delete_specs(&file, &foreign)
            .expect_err("a split cannot name a delete file outside its frozen group");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }
}
