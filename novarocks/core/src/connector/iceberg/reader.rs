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

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    Array, ArrayRef, BooleanArray, DictionaryArray, Int64Array, StringArray, UInt64Array,
    new_null_array,
};
use arrow::compute::{cast, filter_record_batch};
use arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use novarocks_fs::{
    FileBatchReader, FileCancellation, FileError, FileErrorKind, FileFormat, FileIdentity,
    FileMetricsSnapshot, FileProjection, FileReadBudget, FileReadContext, FileReadRange,
    FileReadRequest, FsAccessHandle, MinMaxPredicateOp, MinMaxPredicateValue, PhysicalPruning,
    ScanPredicate, ScanPredicateDomain, ScanPredicateSource, open_file_reader,
};
use novarocks_spi::connector::{
    ConnectorBatchReader, ConnectorError, ConnectorErrorKind, ConnectorOpenReaderRequest,
    ConnectorReaderMetricsSnapshot,
};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use super::delete_file::{IcebergDeleteFileSpec, IcebergFileContent, IcebergFileFormat};
use super::equality_delete::{
    EqualityDeleteSet, equality_delete_keep_mask, load_equality_delete_sets_with_context,
};
use super::position_delete::load_position_deletes_with_context;
use super::scan_model::{
    IcebergDataFileInfo, IcebergDeleteFileContent, IcebergDeleteFileFormat,
    IcebergPhysicalPredicate, IcebergPhysicalPredicateDomain, IcebergPhysicalPredicateOp,
    IcebergPhysicalPredicateValue,
};

pub(crate) struct IcebergBatchReader {
    reader: Box<dyn FileBatchReader>,
    expected_schema: SchemaRef,
    data_file_path: String,
    first_row_id: Option<i64>,
    data_sequence_number: Option<i64>,
    ivm_change_op: Option<i8>,
    position_deletes: roaring::RoaringTreemap,
    equality_deletes: Vec<EqualityDeleteSet>,
    equality_match_only: bool,
    included_positions: Option<roaring::RoaringTreemap>,
    context: novarocks_spi::connector::ConnectorRequestContext,
    cancellation: FileCancellation,
    cancel_on_close: bool,
    closed: bool,
}

impl IcebergBatchReader {
    pub(crate) fn try_new(
        file: &IcebergDataFileInfo,
        physical_predicates: &[IcebergPhysicalPredicate],
        access: FsAccessHandle,
        request: ConnectorOpenReaderRequest,
        file_context: FileReadContext,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_with_equality_mode(
            file,
            physical_predicates,
            access,
            request,
            file_context,
            false,
            true,
        )
    }

    /// Build a reverse-projection reader for an equality-delete delta role.
    /// Ordinary table scans keep rows not present in an equality-delete set;
    /// the delta delete side must deliver exactly those matching rows instead.
    pub(crate) fn try_new_matching_equality(
        file: &IcebergDataFileInfo,
        access: FsAccessHandle,
        request: ConnectorOpenReaderRequest,
        file_context: FileReadContext,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_with_equality_mode(file, &[], access, request, file_context, true, true)
    }

    /// Build one child of a multi-file delta reader.  The parent owns the
    /// shared cancellation token, so EOF for one file must not cancel later
    /// data/delete files in the same connector split.
    pub(crate) fn try_new_delta_child(
        file: &IcebergDataFileInfo,
        access: FsAccessHandle,
        request: ConnectorOpenReaderRequest,
        file_context: FileReadContext,
        equality_match_only: bool,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_with_equality_mode(
            file,
            &[],
            access,
            request,
            file_context,
            equality_match_only,
            false,
        )
    }

    fn try_new_with_equality_mode(
        file: &IcebergDataFileInfo,
        physical_predicates: &[IcebergPhysicalPredicate],
        access: FsAccessHandle,
        request: ConnectorOpenReaderRequest,
        file_context: FileReadContext,
        equality_match_only: bool,
        cancel_on_close: bool,
    ) -> Result<Self, ConnectorError> {
        validate_context(&request.context)?;
        let file_size = u64::try_from(file.size).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                format!("Iceberg data file {} has a negative size", file.path),
            )
        })?;
        // Keep one cancellation token for delete/DV materialization and the
        // physical reader. Connector terminal lifecycle must be able to stop
        // every provider-owned I/O path, not only the data-file decoder.
        let cancellation = file_context.cancellation.clone();
        let delete_specs = delete_specs(file)?;
        let position_deletes =
            load_position_deletes_with_context(&delete_specs, &file.path, &access, &file_context)
                .map_err(|error| ConnectorError::new(ConnectorErrorKind::CorruptData, error))?;
        let equality_deletes =
            load_equality_delete_sets_with_context(&delete_specs, &access, &file_context)
                .map_err(|error| ConnectorError::new(ConnectorErrorKind::CorruptData, error))?;
        let included_positions = included_positions(file)?;
        let bound_file = access
            .bind_location(&file.path, FileIdentity::new(&file.path, file_size, None))
            .map_err(map_file_error)?;
        let format = physical_format(&file.path)?;
        // ORC has no physical predicate support. Its SQL residual stays in
        // Core because Iceberg reports such predicates as Unsupported.
        let predicates = if format == FileFormat::Parquet {
            physical_predicates_to_file_predicates(physical_predicates)
        } else {
            Vec::new()
        };
        let reader = open_file_reader(FileReadRequest {
            file: bound_file,
            format,
            range: FileReadRange::WholeFile,
            projection: FileProjection::All,
            budget: FileReadBudget {
                max_rows: request.batch.max_rows,
                max_bytes: request.batch.max_bytes,
            },
            predicates,
            pruning: PhysicalPruning::default(),
            cache: None,
            context: file_context,
        })
        .map_err(map_file_error)?;
        Ok(Self {
            reader,
            expected_schema: request.expected_schema,
            data_file_path: file.path.clone(),
            first_row_id: file.first_row_id,
            data_sequence_number: file.data_sequence_number,
            ivm_change_op: file.ivm_change_op,
            position_deletes,
            equality_deletes,
            equality_match_only,
            included_positions,
            context: request.context,
            cancellation,
            cancel_on_close,
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

fn physical_predicates_to_file_predicates(
    predicates: &[IcebergPhysicalPredicate],
) -> Vec<ScanPredicate> {
    predicates
        .iter()
        .filter_map(|predicate| {
            let value = |value: &IcebergPhysicalPredicateValue| match value {
                IcebergPhysicalPredicateValue::Boolean(value) => {
                    MinMaxPredicateValue::Boolean(*value)
                }
                IcebergPhysicalPredicateValue::Int32(value) => MinMaxPredicateValue::Int32(*value),
                IcebergPhysicalPredicateValue::Int64(value) => MinMaxPredicateValue::Int64(*value),
                // Parquet exposes DATE statistics as INT32 day counts.
                IcebergPhysicalPredicateValue::Date32(value) => MinMaxPredicateValue::Int32(*value),
            };
            let domain = match &predicate.domain {
                IcebergPhysicalPredicateDomain::Range { op, value: literal } => {
                    ScanPredicateDomain::Range {
                        op: match op {
                            IcebergPhysicalPredicateOp::Eq => MinMaxPredicateOp::Eq,
                            IcebergPhysicalPredicateOp::Lt => MinMaxPredicateOp::Lt,
                            IcebergPhysicalPredicateOp::Le => MinMaxPredicateOp::Le,
                            IcebergPhysicalPredicateOp::Gt => MinMaxPredicateOp::Gt,
                            IcebergPhysicalPredicateOp::Ge => MinMaxPredicateOp::Ge,
                        },
                        value: value(literal),
                    }
                }
                IcebergPhysicalPredicateDomain::DiscreteSet { values } => {
                    let values = values.iter().map(value).collect::<Vec<_>>();
                    if values.is_empty() {
                        return None;
                    }
                    let min = values.first()?.clone();
                    let max = values.last()?.clone();
                    ScanPredicateDomain::DiscreteSet { values, min, max }
                }
            };
            Some(
                ScanPredicate::new(
                    predicate.column.clone(),
                    domain,
                    ScanPredicateSource::Static,
                )
                .with_physical_field_id(predicate.field_id),
            )
        })
        .collect()
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
            Some(file_batch) => {
                let (batch, positions) = apply_delete_filters(
                    file_batch.batch,
                    file_batch.physical_row_positions,
                    &self.position_deletes,
                    &self.equality_deletes,
                    self.equality_match_only,
                    self.included_positions.as_ref(),
                    &self.data_file_path,
                )?;
                let batch = align_batch_to_schema(
                    &self.expected_schema,
                    batch,
                    positions.as_ref(),
                    IcebergFileFacts {
                        path: &self.data_file_path,
                        first_row_id: self.first_row_id,
                        data_sequence_number: self.data_sequence_number,
                        ivm_change_op: self.ivm_change_op,
                    },
                )
                .map_err(|error| ConnectorError::new(ConnectorErrorKind::CorruptData, error))?;
                dictionary_encode_low_cardinality_strings(batch)
                    .map(Some)
                    .map_err(|error| ConnectorError::new(ConnectorErrorKind::CorruptData, error))
            }
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
        if self.cancel_on_close {
            self.cancellation.cancel();
        }
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

pub(super) fn validate_delete_apply_cost(file: &IcebergDataFileInfo) -> Result<(), ConnectorError> {
    const MAX_DELETE_FILES: usize = 1024;
    const MAX_DELETE_BYTES: i64 = 512 * 1024 * 1024;
    if file.delete_files.len() > MAX_DELETE_FILES {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            format!(
                "too many Iceberg delete files attached to {}: count={} max={MAX_DELETE_FILES}",
                file.path,
                file.delete_files.len()
            ),
        ));
    }
    let total_bytes = file
        .delete_files
        .iter()
        .try_fold(0_i64, |total, delete| {
            total.checked_add(delete.length.unwrap_or_default().max(0))
        })
        .ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                format!("Iceberg delete byte total overflows for {}", file.path),
            )
        })?;
    if total_bytes > MAX_DELETE_BYTES {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            format!(
                "Iceberg delete files attached to {} exceed byte limit: bytes={total_bytes} max={MAX_DELETE_BYTES}",
                file.path
            ),
        ));
    }
    Ok(())
}

fn delete_specs(file: &IcebergDataFileInfo) -> Result<Vec<IcebergDeleteFileSpec>, ConnectorError> {
    validate_delete_apply_cost(file)?;
    file.delete_files
        .iter()
        .map(|delete| {
            let file_format = match delete.file_format {
                IcebergDeleteFileFormat::Parquet => IcebergFileFormat::Parquet,
                IcebergDeleteFileFormat::Puffin => IcebergFileFormat::Puffin,
            };
            let file_content = match delete.file_content {
                IcebergDeleteFileContent::Position => IcebergFileContent::PositionDeletes,
                IcebergDeleteFileContent::Equality => IcebergFileContent::EqualityDeletes,
            };
            Ok(IcebergDeleteFileSpec {
                path: delete.path.clone(),
                file_format,
                file_content,
                length: delete.length.and_then(|length| u64::try_from(length).ok()),
                content_offset: delete.content_offset,
                content_size_in_bytes: delete.content_size_in_bytes,
            })
        })
        .collect()
}

fn included_positions(
    file: &IcebergDataFileInfo,
) -> Result<Option<roaring::RoaringTreemap>, ConnectorError> {
    let Some(positions) = &file.included_positions else {
        return Ok(None);
    };
    let mut included = roaring::RoaringTreemap::new();
    for position in positions {
        let position = u64::try_from(*position).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                format!("Iceberg included position is negative for {}", file.path),
            )
        })?;
        included.insert(position);
    }
    Ok(Some(included))
}

struct IcebergFileFacts<'a> {
    path: &'a str,
    first_row_id: Option<i64>,
    data_sequence_number: Option<i64>,
    ivm_change_op: Option<i8>,
}

fn apply_delete_filters(
    batch: RecordBatch,
    positions: Option<UInt64Array>,
    position_deletes: &roaring::RoaringTreemap,
    equality_deletes: &[EqualityDeleteSet],
    equality_match_only: bool,
    included_positions: Option<&roaring::RoaringTreemap>,
    data_file_path: &str,
) -> Result<(RecordBatch, Option<UInt64Array>), ConnectorError> {
    if batch.num_rows() == 0 {
        return Ok((batch, positions));
    }
    let positions = positions.ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            format!("Iceberg physical reader did not return row coordinates for {data_file_path}"),
        )
    })?;
    if positions.len() != batch.num_rows() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            format!(
                "Iceberg physical position count mismatch for {data_file_path}: positions={} rows={}",
                positions.len(),
                batch.num_rows()
            ),
        ));
    }
    let equality_keep = equality_delete_keep_mask(&batch, equality_deletes)
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::CorruptData, error))?;
    let changed = equality_match_only
        || equality_keep.is_some()
        || !position_deletes.is_empty()
        || included_positions.is_some();
    let mut keep = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let position = positions.value(row);
        let position_keep = !position_deletes.contains(position)
            && included_positions.is_none_or(|included| included.contains(position));
        let normal_equality_keep = match equality_keep.as_ref() {
            Some(values) => values.get(row).copied().unwrap_or(false),
            None => true,
        };
        let equality_keep = if equality_match_only {
            !normal_equality_keep
        } else {
            normal_equality_keep
        };
        keep.push(position_keep && equality_keep);
    }
    if !changed {
        return Ok((batch, Some(positions)));
    }
    if keep.iter().all(|keep| *keep) {
        return Ok((batch, Some(positions)));
    }
    let keep = BooleanArray::from(keep);
    let batch = filter_record_batch(&batch, &keep).map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            format!("apply Iceberg delete filter for {data_file_path}: {error}"),
        )
    })?;
    let positions = arrow::compute::filter(&positions, &keep).map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            format!("filter Iceberg physical positions for {data_file_path}: {error}"),
        )
    })?;
    let positions = positions
        .as_any()
        .downcast_ref::<UInt64Array>()
        .cloned()
        .ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "filtered Iceberg positions changed type",
            )
        })?;
    Ok((batch, Some(positions)))
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

fn align_batch_to_schema(
    expected: &SchemaRef,
    batch: RecordBatch,
    positions: Option<&UInt64Array>,
    facts: IcebergFileFacts<'_>,
) -> Result<RecordBatch, String> {
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
                    } else if matches!(target.data_type(), DataType::LargeBinary)
                        && crate::formats::parquet::is_variant_struct_data_type(source.data_type())
                    {
                        crate::formats::parquet::collapse_variant_struct_to_largebinary(
                            &source,
                            target.name(),
                        )?
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
                None if is_iceberg_virtual(target.name()) => {
                    iceberg_virtual_column(target, batch.num_rows(), positions, &facts)?
                }
                None if target.metadata().contains_key(
                    crate::connector::iceberg::schema::ICEBERG_INITIAL_DEFAULT_META_KEY,
                ) =>
                {
                    crate::formats::parquet::build_iceberg_default_array(
                        target.as_ref(),
                        batch.num_rows(),
                    )?
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

/// Retain a low-cardinality UTF-8 scan column as an Arrow dictionary carrier.
///
/// Parquet decoding normally materializes dictionary pages as flat UTF-8.  Keeping
/// small domains encoded lets downstream filter and aggregate operators avoid
/// repeatedly processing the same string values while retaining the logical scan
/// schema.  High-cardinality columns stay flat so the per-batch dictionary does
/// not inflate memory use.
fn dictionary_encode_low_cardinality_strings(batch: RecordBatch) -> Result<RecordBatch, String> {
    const MAX_DISTINCT_VALUES: usize = 64;
    const MIN_NON_NULL_VALUES: usize = 2;

    let mut changed = false;
    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        let encoded = match column.data_type() {
            DataType::Utf8 => {
                let values = column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| "failed to downcast Iceberg UTF-8 column".to_string())?;
                let mut distinct = HashSet::new();
                let mut non_null = 0usize;
                for row in 0..values.len() {
                    if !values.is_null(row) {
                        non_null += 1;
                        distinct.insert(values.value(row));
                    }
                }
                (non_null >= MIN_NON_NULL_VALUES
                    && distinct.len() <= MAX_DISTINCT_VALUES
                    && distinct.len().saturating_mul(5) <= non_null.saturating_mul(4))
                .then(|| {
                    Arc::new(
                        (0..values.len())
                            .map(|row| (!values.is_null(row)).then(|| values.value(row)))
                            .collect::<DictionaryArray<Int32Type>>(),
                    ) as ArrayRef
                })
            }
            _ => None,
        };
        if let Some(encoded) = encoded {
            changed = true;
            fields.push(Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(encoded.data_type().clone()),
            ));
            columns.push(encoded);
        } else {
            fields.push(field.clone());
            columns.push(column.clone());
        }
    }
    if !changed {
        return Ok(batch);
    }
    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(
            fields,
            batch.schema().metadata().clone(),
        )),
        columns,
    )
    .map_err(|error| format!("build dictionary-encoded Iceberg batch failed: {error}"))
}

fn is_iceberg_virtual(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "_file" | "_pos" | "_row_id" | "_last_updated_sequence_number" | "__change_op"
    )
}

fn iceberg_virtual_column(
    target: &Field,
    row_count: usize,
    positions: Option<&UInt64Array>,
    facts: &IcebergFileFacts<'_>,
) -> Result<ArrayRef, String> {
    let name = target.name().to_ascii_lowercase();
    let require_positions = || -> Result<&UInt64Array, String> {
        let positions = positions.ok_or_else(|| {
            format!(
                "Iceberg virtual column {} requires physical row coordinates for {}",
                target.name(),
                facts.path
            )
        })?;
        if positions.len() != row_count {
            return Err(format!(
                "Iceberg virtual column {} position count {} differs from row count {row_count}",
                target.name(),
                positions.len()
            ));
        }
        Ok(positions)
    };
    let raw: ArrayRef = match name.as_str() {
        "_file" => Arc::new(StringArray::from(vec![facts.path; row_count])),
        "_pos" => Arc::new(Int64Array::from(
            require_positions()?
                .iter()
                .map(|value| {
                    value
                        .map(|value| {
                            i64::try_from(value).map_err(|_| "Iceberg row position exceeds Int64")
                        })
                        .transpose()
                })
                .collect::<Result<Vec<Option<i64>>, _>>()?,
        )),
        "_row_id" => match facts.first_row_id {
            Some(first) => Arc::new(Int64Array::from(
                require_positions()?
                    .iter()
                    .map(|value| {
                        value
                            .ok_or("Iceberg physical row position is null")
                            .and_then(|value| {
                                i64::try_from(value)
                                    .map_err(|_| "Iceberg row position exceeds Int64")
                            })
                            .and_then(|value| {
                                first
                                    .checked_add(value)
                                    .ok_or("Iceberg row id overflows Int64")
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            None if target.is_nullable() => new_null_array(target.data_type(), row_count),
            None => {
                return Err(format!(
                    "Iceberg data file {} is missing first_row_id for _row_id",
                    facts.path
                ));
            }
        },
        "_last_updated_sequence_number" => match facts.data_sequence_number {
            Some(sequence) => Arc::new(Int64Array::from(vec![sequence; row_count])),
            None if target.is_nullable() => new_null_array(target.data_type(), row_count),
            None => {
                return Err(format!(
                    "Iceberg data file {} is missing data_sequence_number",
                    facts.path
                ));
            }
        },
        "__change_op" => match facts.ivm_change_op {
            Some(change_op) => Arc::new(arrow::array::Int8Array::from(vec![change_op; row_count])),
            None if target.is_nullable() => new_null_array(target.data_type(), row_count),
            None => {
                return Err(format!(
                    "Iceberg data file {} is missing ivm_change_op for __change_op",
                    facts.path
                ));
            }
        },
        _ => unreachable!("virtual column was validated by caller"),
    };
    if raw.data_type() == target.data_type() {
        Ok(raw)
    } else {
        cast(raw.as_ref(), target.data_type()).map_err(|error| {
            format!(
                "Iceberg virtual column {} cannot cast to {:?}: {error}",
                target.name(),
                target.data_type()
            )
        })
    }
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

        let aligned = align_batch_to_schema(
            &expected,
            source,
            None,
            IcebergFileFacts {
                path: "file:///test.parquet",
                first_row_id: None,
                data_sequence_number: None,
                ivm_change_op: None,
            },
        )
        .expect("field-ID alignment");
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

    #[test]
    fn aligns_iceberg_output_with_initial_default_for_missing_field() {
        let source = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field_with_id("id", 1, false)])),
            vec![Arc::new(Int32Array::from(vec![7])) as ArrayRef],
        )
        .expect("source batch");
        let expected = Arc::new(Schema::new(vec![
            field_with_id("id", 1, false),
            Field::new("introduced", DataType::Int32, true).with_metadata(HashMap::from([
                (PARQUET_FIELD_ID_META_KEY.to_string(), "2".to_string()),
                (
                    crate::connector::iceberg::schema::ICEBERG_INITIAL_DEFAULT_META_KEY.to_string(),
                    "5".to_string(),
                ),
            ])),
        ]));

        let aligned = align_batch_to_schema(
            &expected,
            source,
            None,
            IcebergFileFacts {
                path: "file:///test.parquet",
                first_row_id: None,
                data_sequence_number: None,
                ivm_change_op: None,
            },
        )
        .expect("initial default must fill older data files");

        let introduced = aligned
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("introduced column");
        assert_eq!(introduced.value(0), 5);
        assert_eq!(introduced.null_count(), 0);
    }

    #[test]
    fn dictionary_encodes_low_cardinality_utf8_columns() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "status",
                DataType::Utf8,
                true,
            )])),
            vec![Arc::new(StringArray::from(vec![
                Some("NEW"),
                Some("PAID"),
                Some("PAID"),
                Some("CANCELLED"),
                Some("SHIPPED"),
                None,
            ])) as ArrayRef],
        )
        .expect("source batch");

        let encoded = dictionary_encode_low_cardinality_strings(batch)
            .expect("dictionary encode low-cardinality status");
        assert!(matches!(
            encoded.column(0).data_type(),
            DataType::Dictionary(key, value)
                if key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::Utf8
        ));
    }

    #[test]
    fn applies_position_deletes_and_included_positions_to_physical_coordinates() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef],
        )
        .expect("source batch");
        let deletes = roaring::RoaringTreemap::from_iter([6]);
        let included = roaring::RoaringTreemap::from_iter([5, 6]);

        let (filtered, positions) = apply_delete_filters(
            batch,
            Some(UInt64Array::from(vec![5, 6, 7])),
            &deletes,
            &[],
            false,
            Some(&included),
            "file:///warehouse/data.parquet",
        )
        .expect("filter physical positions");
        assert_eq!(filtered.num_rows(), 1);
        assert_eq!(positions.unwrap().value(0), 5);
        assert_eq!(
            filtered
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            10
        );
    }

    #[test]
    fn synthesizes_virtual_and_lineage_columns_from_filtered_positions() {
        let source = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field_with_id("id", 1, false)])),
            vec![Arc::new(Int32Array::from(vec![10, 30])) as ArrayRef],
        )
        .unwrap();
        let expected = Arc::new(Schema::new(vec![
            field_with_id("id", 1, false),
            Field::new("_file", DataType::Utf8, false),
            Field::new("_pos", DataType::Int64, false),
            Field::new("_row_id", DataType::Int64, false),
            Field::new("_last_updated_sequence_number", DataType::Int64, false),
        ]));
        let aligned = align_batch_to_schema(
            &expected,
            source,
            Some(&UInt64Array::from(vec![5, 7])),
            IcebergFileFacts {
                path: "s3://warehouse/data.parquet",
                first_row_id: Some(100),
                data_sequence_number: Some(19),
                ivm_change_op: None,
            },
        )
        .unwrap();
        assert_eq!(
            aligned
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "s3://warehouse/data.parquet"
        );
        assert_eq!(
            aligned
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[5, 7]
        );
        assert_eq!(
            aligned
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[105, 107]
        );
        assert_eq!(
            aligned
                .column(4)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[19, 19]
        );
    }

    #[test]
    fn synthesizes_delta_change_op_from_provider_file_facts() {
        let source = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field_with_id("id", 1, false)])),
            vec![Arc::new(Int32Array::from(vec![10, 30])) as ArrayRef],
        )
        .unwrap();
        let expected = Arc::new(Schema::new(vec![
            field_with_id("id", 1, false),
            Field::new("__change_op", DataType::Int8, false),
        ]));
        let aligned = align_batch_to_schema(
            &expected,
            source,
            None,
            IcebergFileFacts {
                path: "s3://warehouse/data.parquet",
                first_row_id: None,
                data_sequence_number: None,
                ivm_change_op: Some(1),
            },
        )
        .unwrap();
        assert_eq!(
            aligned
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .unwrap()
                .values(),
            &[1, 1]
        );
    }
}
