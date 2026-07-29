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

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::UInt64Array;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::file::statistics::Statistics;

use super::chunk_reader::{BoundChunkReader, ReaderMetrics};
use crate::{
    FileBatch, FileBatchReader, FileError, FileErrorKind, FileMetricsSnapshot, FileProjection,
    FileReadRange, FileReadRequest, FileResult, MinMaxPredicateOp, MinMaxPredicateValue,
    ScanPredicate, ScanPredicateDomain,
};

pub(crate) struct ParquetPhysicalReader {
    reader: Option<ParquetRecordBatchReader>,
    positions: VecDeque<PositionSpan>,
    context: crate::FileReadContext,
    metrics: Arc<ReaderMetrics>,
    closed: bool,
}

#[derive(Clone, Copy, Debug)]
struct PositionSpan {
    next: u64,
    remaining: usize,
}

impl ParquetPhysicalReader {
    pub(crate) fn try_new(request: FileReadRequest) -> FileResult<Self> {
        request.context.check_active()?;
        if !request.pruning.pages.is_empty() {
            return Err(FileError::unsupported(
                "explicit Parquet page selections are not supported by the physical reader",
            ));
        }

        let metrics = Arc::new(ReaderMetrics::default());
        let chunk_reader = BoundChunkReader::new(
            request.file,
            request.context.clone(),
            request.cache,
            Arc::clone(&metrics),
        );
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(chunk_reader)
            .map_err(|error| parquet_error("open Parquet metadata", error))?;
        request.context.check_active()?;

        let projection = projection_mask(&builder, &request.projection)?;
        builder = builder
            .with_projection(projection)
            .with_batch_size(request.budget.max_rows.get());

        let metadata = builder.metadata().clone();
        let row_groups = select_row_groups(
            metadata.as_ref(),
            request.range,
            request.pruning.row_groups.as_deref(),
            &request.predicates,
        );
        let positions = row_position_spans(metadata.as_ref(), &row_groups)?;
        builder = builder.with_row_groups(row_groups);
        let reader = builder
            .build()
            .map_err(|error| parquet_error("build Parquet reader", error))?;

        Ok(Self {
            reader: Some(reader),
            positions,
            context: request.context,
            metrics,
            closed: false,
        })
    }

    fn take_positions(&mut self, count: usize) -> FileResult<UInt64Array> {
        let mut output = Vec::with_capacity(count);
        while output.len() < count {
            let Some(span) = self.positions.front_mut() else {
                return Err(FileError::new(
                    FileErrorKind::Corrupt,
                    "Parquet decoder produced more rows than selected row-group metadata",
                ));
            };
            let take = span.remaining.min(count - output.len());
            output.extend(span.next..span.next + take as u64);
            span.next += take as u64;
            span.remaining -= take;
            if span.remaining == 0 {
                self.positions.pop_front();
            }
        }
        Ok(UInt64Array::from(output))
    }
}

impl FileBatchReader for ParquetPhysicalReader {
    fn next_batch(&mut self) -> FileResult<Option<FileBatch>> {
        if self.closed {
            return Ok(None);
        }
        self.context.check_active()?;
        let began = Instant::now();
        let next = self
            .reader
            .as_mut()
            .and_then(Iterator::next)
            .transpose()
            .map_err(|error| format_error("decode Parquet batch", error))?;
        self.context.check_active()?;
        let Some(batch) = next else {
            self.close()?;
            return Ok(None);
        };
        let positions = self.take_positions(batch.num_rows())?;
        self.metrics
            .record_decode(batch.num_rows(), began.elapsed().as_nanos());
        self.metrics.record_delivery();
        Ok(Some(FileBatch {
            batch,
            physical_row_positions: Some(positions),
        }))
    }

    fn close(&mut self) -> FileResult<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        self.reader = None;
        self.positions.clear();
        Ok(())
    }

    fn metrics_snapshot(&self) -> FileMetricsSnapshot {
        self.metrics.snapshot()
    }
}

impl Drop for ParquetPhysicalReader {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

fn projection_mask(
    builder: &ParquetRecordBatchReaderBuilder<BoundChunkReader>,
    projection: &FileProjection,
) -> FileResult<ProjectionMask> {
    let parquet_schema = builder.parquet_schema();
    let arrow_schema = builder.schema();
    let roots = match projection {
        FileProjection::All => return Ok(ProjectionMask::all()),
        FileProjection::RootNames(names) => {
            let by_name = arrow_schema
                .fields()
                .iter()
                .enumerate()
                .map(|(index, field)| (field.name().as_str(), index))
                .collect::<HashMap<_, _>>();
            names
                .iter()
                .map(|name| {
                    by_name.get(name.as_str()).copied().ok_or_else(|| {
                        FileError::invalid(format!(
                            "Parquet projection column does not exist: {name}"
                        ))
                    })
                })
                .collect::<FileResult<Vec<_>>>()?
        }
        FileProjection::RootIndices(indices) => {
            for index in indices {
                if *index >= arrow_schema.fields().len() {
                    return Err(FileError::invalid(format!(
                        "Parquet root projection index out of bounds: {index}"
                    )));
                }
            }
            indices.clone()
        }
        FileProjection::FieldIds(field_ids) => {
            let wanted = field_ids.iter().copied().collect::<HashSet<_>>();
            let mut found = HashSet::new();
            let mut roots = Vec::new();
            for (index, field) in parquet_schema.root_schema().get_fields().iter().enumerate() {
                let info = field.get_basic_info();
                if info.has_id() && wanted.contains(&info.id()) {
                    roots.push(index);
                    found.insert(info.id());
                }
            }
            if found.len() != wanted.len() {
                let mut missing = wanted.difference(&found).copied().collect::<Vec<_>>();
                missing.sort_unstable();
                return Err(FileError::invalid(format!(
                    "Parquet field-ID projection contains unknown IDs: {missing:?}"
                )));
            }
            roots
        }
    };
    Ok(ProjectionMask::roots(parquet_schema, roots))
}

fn select_row_groups(
    metadata: &ParquetMetaData,
    range: FileReadRange,
    explicit: Option<&[usize]>,
    predicates: &[ScanPredicate],
) -> Vec<usize> {
    let explicit = explicit.map(|groups| groups.iter().copied().collect::<HashSet<_>>());
    metadata
        .row_groups()
        .iter()
        .enumerate()
        .filter(|(index, row_group)| {
            explicit
                .as_ref()
                .is_none_or(|groups| groups.contains(index))
                && row_group_in_range(row_group, range)
                && row_group_may_match(row_group, predicates)
        })
        .map(|(index, _)| index)
        .collect()
}

fn row_group_in_range(row_group: &RowGroupMetaData, range: FileReadRange) -> bool {
    let FileReadRange::Bounded { offset, length } = range else {
        return true;
    };
    let end = offset.saturating_add(length);
    row_group_start_offset(row_group).is_none_or(|start| start >= offset && start < end)
}

fn row_group_start_offset(row_group: &RowGroupMetaData) -> Option<u64> {
    row_group
        .columns()
        .first()
        .map(|column| {
            column
                .dictionary_page_offset()
                .unwrap_or_else(|| column.data_page_offset())
                .min(column.data_page_offset())
        })
        .and_then(|offset| u64::try_from(offset).ok())
}

fn row_position_spans(
    metadata: &ParquetMetaData,
    selected: &[usize],
) -> FileResult<VecDeque<PositionSpan>> {
    let selected = selected.iter().copied().collect::<HashSet<_>>();
    let mut first_row = 0u64;
    let mut spans = VecDeque::new();
    for (index, row_group) in metadata.row_groups().iter().enumerate() {
        let rows = usize::try_from(row_group.num_rows()).map_err(|_| {
            FileError::new(
                FileErrorKind::Corrupt,
                "negative or overflowing Parquet row-group row count",
            )
        })?;
        if selected.contains(&index) {
            spans.push_back(PositionSpan {
                next: first_row,
                remaining: rows,
            });
        }
        first_row = first_row
            .checked_add(rows as u64)
            .ok_or_else(|| FileError::new(FileErrorKind::Corrupt, "Parquet row count overflow"))?;
    }
    Ok(spans)
}

fn row_group_may_match(row_group: &RowGroupMetaData, predicates: &[ScanPredicate]) -> bool {
    predicates.iter().all(|predicate| {
        let column = row_group.columns().iter().find(|column| {
            column
                .column_path()
                .parts()
                .first()
                .is_some_and(|name| name == predicate.column())
        });
        let Some(statistics) = column.and_then(|column| column.statistics()) else {
            return true;
        };
        predicate_may_match(statistics, predicate.domain())
    })
}

fn predicate_may_match(statistics: &Statistics, domain: &ScanPredicateDomain) -> bool {
    let Some((min, max)) = statistic_bounds(statistics) else {
        return true;
    };
    match domain {
        ScanPredicateDomain::Range { op, value } => match op {
            MinMaxPredicateOp::Le => {
                compare(&min, value).is_some_and(|order| order != Ordering::Greater)
            }
            MinMaxPredicateOp::Lt => {
                compare(&min, value).is_some_and(|order| order == Ordering::Less)
            }
            MinMaxPredicateOp::Ge => {
                compare(&max, value).is_some_and(|order| order != Ordering::Less)
            }
            MinMaxPredicateOp::Gt => {
                compare(&max, value).is_some_and(|order| order == Ordering::Greater)
            }
            MinMaxPredicateOp::Eq => {
                compare(&min, value).is_some_and(|order| order != Ordering::Greater)
                    && compare(&max, value).is_some_and(|order| order != Ordering::Less)
            }
        },
        ScanPredicateDomain::DiscreteSet { values, .. }
        | ScanPredicateDomain::Membership { values } => values.iter().any(|value| {
            compare(&min, value).is_some_and(|order| order != Ordering::Greater)
                && compare(&max, value).is_some_and(|order| order != Ordering::Less)
        }),
    }
}

fn statistic_bounds(
    statistics: &Statistics,
) -> Option<(MinMaxPredicateValue, MinMaxPredicateValue)> {
    match statistics {
        Statistics::Boolean(value) => Some((
            MinMaxPredicateValue::Boolean(*value.min_opt()?),
            MinMaxPredicateValue::Boolean(*value.max_opt()?),
        )),
        Statistics::Int32(value) => Some((
            MinMaxPredicateValue::Int32(*value.min_opt()?),
            MinMaxPredicateValue::Int32(*value.max_opt()?),
        )),
        Statistics::Int64(value) => Some((
            MinMaxPredicateValue::Int64(*value.min_opt()?),
            MinMaxPredicateValue::Int64(*value.max_opt()?),
        )),
        Statistics::Float(value) => Some((
            MinMaxPredicateValue::Float(*value.min_opt()?),
            MinMaxPredicateValue::Float(*value.max_opt()?),
        )),
        Statistics::Double(value) => Some((
            MinMaxPredicateValue::Double(*value.min_opt()?),
            MinMaxPredicateValue::Double(*value.max_opt()?),
        )),
        Statistics::ByteArray(value) => Some((
            MinMaxPredicateValue::ByteArray(value.min_opt()?.data().to_vec()),
            MinMaxPredicateValue::ByteArray(value.max_opt()?.data().to_vec()),
        )),
        Statistics::FixedLenByteArray(value) => Some((
            MinMaxPredicateValue::FixedLenByteArray(value.min_opt()?.data().to_vec()),
            MinMaxPredicateValue::FixedLenByteArray(value.max_opt()?.data().to_vec()),
        )),
        _ => None,
    }
}

fn compare(left: &MinMaxPredicateValue, right: &MinMaxPredicateValue) -> Option<Ordering> {
    match (left, right) {
        (MinMaxPredicateValue::Boolean(a), MinMaxPredicateValue::Boolean(b)) => a.partial_cmp(b),
        (MinMaxPredicateValue::Int32(a), MinMaxPredicateValue::Int32(b)) => a.partial_cmp(b),
        (MinMaxPredicateValue::Int64(a), MinMaxPredicateValue::Int64(b)) => a.partial_cmp(b),
        (MinMaxPredicateValue::Float(a), MinMaxPredicateValue::Float(b)) => a.partial_cmp(b),
        (MinMaxPredicateValue::Double(a), MinMaxPredicateValue::Double(b)) => a.partial_cmp(b),
        (MinMaxPredicateValue::ByteArray(a), MinMaxPredicateValue::ByteArray(b))
        | (
            MinMaxPredicateValue::FixedLenByteArray(a),
            MinMaxPredicateValue::FixedLenByteArray(b),
        ) => a.partial_cmp(b),
        _ => None,
    }
}

fn parquet_error(operation: &'static str, error: parquet::errors::ParquetError) -> FileError {
    let message = error.to_string();
    let kind = if message.contains("Cancelled:") {
        FileErrorKind::Cancelled
    } else if message.contains("DeadlineExceeded:") {
        FileErrorKind::DeadlineExceeded
    } else {
        FileErrorKind::Corrupt
    };
    FileError::with_source(kind, format!("{operation} failed"), error)
}

fn format_error(
    operation: &'static str,
    error: impl std::error::Error + Send + Sync + 'static,
) -> FileError {
    let message = error.to_string();
    let kind = if message.contains("Cancelled:") {
        FileErrorKind::Cancelled
    } else if message.contains("DeadlineExceeded:") {
        FileErrorKind::DeadlineExceeded
    } else {
        FileErrorKind::Corrupt
    };
    FileError::with_source(kind, format!("{operation} failed"), error)
}
