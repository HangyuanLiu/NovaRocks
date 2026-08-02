// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.  The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! BE-only source for a frozen Iceberg Puffin deletion-vector rewrite group.
//!
//! The Iceberg control provider creates the opaque split after validating the
//! immutable group artifact.  This reader has only the startup-bound object
//! store capability; it converts the selected Puffin ranges into the ordinary
//! `(_file, _pos)` batches consumed by C1's existing deletion-vector writer.

use std::collections::VecDeque;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use iceberg::spec::DataFileFormat;
use novarocks_spi::connector::{
    ConnectorBatchReader, ConnectorError, ConnectorErrorKind, ConnectorOpenReaderRequest,
    ConnectorReaderMetricsSnapshot,
};

use super::changes::PositionDeleteRef;
use super::provider::{IcebergReadBinding, IcebergRewritePositionSplitPayloadV1};
use super::scan_model::{IcebergDataFileInfo, IcebergDeleteFileContent, IcebergDeleteFileFormat};

const POSITION_BATCH_ROWS: usize = 64 * 1024;

pub(crate) struct IcebergRewritePositionBatchReader {
    request: ConnectorOpenReaderRequest,
    batches: VecDeque<RecordBatch>,
    metrics: ConnectorReaderMetricsSnapshot,
    closed: bool,
}

impl IcebergRewritePositionBatchReader {
    pub(crate) fn try_new(
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
        let refs = selected_refs(&data_file, &payload)?;
        let access = binding.resolve_access_for_locations(
            refs.iter()
                .map(|reference| reference.delete_file_path.as_str()),
        )?;
        let read_bytes = refs
            .iter()
            .map(|reference| reference.content_size_in_bytes.unwrap_or(0).max(0) as u64)
            .sum();
        let positions = crate::runtime::global_async_runtime::data_block_on(async {
            super::scan_deletes::read_dv_positions_per_data_file(&refs, &access).await
        })
        .map_err(|error| unavailable(error))?
        .map_err(|error| corrupt(error.to_string()))?;
        let positions = positions.get(&data_file.path).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "Iceberg rewrite-position Puffin files do not reference the frozen data file",
            )
        })?;
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
                read_requests: refs.len() as u64,
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

fn selected_refs(
    data_file: &IcebergDataFileInfo,
    payload: &IcebergRewritePositionSplitPayloadV1,
) -> Result<Vec<PositionDeleteRef>, ConnectorError> {
    if payload.selected_delete_files.is_empty() {
        return Err(corrupt(
            "Iceberg rewrite-position split has no selected Puffin files",
        ));
    }
    let mut refs = Vec::with_capacity(payload.selected_delete_files.len());
    for delete in &payload.selected_delete_files {
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
        let reference = PositionDeleteRef {
            delete_file_path: delete.path.clone(),
            delete_file_size: delete.length.unwrap_or(0),
            record_count: None,
            referenced_data_file: Some(data_file.path.clone()),
            file_format: DataFileFormat::Puffin,
            content_offset: delete.content_offset,
            content_size_in_bytes: delete.content_size_in_bytes,
            partition_values: Vec::new(),
        };
        reference
            .validate_invariants()
            .map_err(|error| corrupt(error.to_string()))?;
        refs.push(reference);
    }
    Ok(refs)
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

fn unavailable(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message.into())
        .with_retryable_before_progress()
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
    use crate::connector::iceberg::scan_model::IcebergDeleteFileInfo;

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
    fn selected_refs_rejects_a_delete_file_outside_the_frozen_data_file() {
        let selected = puffin("s3://warehouse/delete-1.puffin");
        let mut file = IcebergDataFileInfo::for_test("s3://warehouse/data-1.parquet", 16, 2);
        file.delete_files.push(selected.clone());
        let payload = IcebergRewritePositionSplitPayloadV1 {
            version: 1,
            selected_delete_files: vec![selected],
        };
        let refs = selected_refs(&file, &payload).expect("frozen Puffin reference");
        assert_eq!(refs.len(), 1);
        assert_eq!(
            refs[0].referenced_data_file.as_deref(),
            Some("s3://warehouse/data-1.parquet")
        );

        let foreign = IcebergRewritePositionSplitPayloadV1 {
            version: 1,
            selected_delete_files: vec![puffin("s3://warehouse/delete-foreign.puffin")],
        };
        let error = selected_refs(&file, &foreign)
            .expect_err("a split cannot name a delete file outside its frozen group");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }
}
