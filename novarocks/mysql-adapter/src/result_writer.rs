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

//! MySQL schema-start and row-writer adaptation for ungoverned results.

use std::io;

use arrow::record_batch::RecordBatch;
use novarocks_query_application::api::{
    QueryExecutionError, QueryExecutionErrorKind, ResultFailureView, ResultField,
};
use novarocks_query_application::cancellation::{QueryCancellationReason, QueryCancellationView};
use opensrv_mysql::{Column, ErrorKind, QueryResultWriter};
use tokio::io::AsyncWrite;

use crate::{build_mysql_row, mysql_column_for_result_field};

/// Writes already-materialized Arrow batches as one ordinary MySQL result.
///
/// The caller retains ownership of the result carrier. The adapter owns every
/// wire-visible transition after it receives the immutable result schema.
pub async fn write_record_batches<W: AsyncWrite + Unpin>(
    fields: &[ResultField],
    batches: &[&RecordBatch],
    results: QueryResultWriter<'_, W>,
) -> io::Result<()> {
    write_record_batches_one(fields, batches, results)
        .await?
        .no_more_results()
        .await
}

/// Writes one ordinary result and returns the MySQL writer so a negotiated
/// multi-statement request can continue with its next result.
pub async fn write_record_batches_one<'writer, W: AsyncWrite + Unpin>(
    fields: &[ResultField],
    batches: &[&RecordBatch],
    results: QueryResultWriter<'writer, W>,
) -> io::Result<QueryResultWriter<'writer, W>> {
    let columns = mysql_columns_for_result_fields(fields)?;
    let mut writer = results.start(columns.as_slice()).await?;
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            writer
                .write_row(build_mysql_row(batch, fields, row_idx).map_err(invalid_data_error)?)
                .await?;
        }
    }
    writer.finish_one().await
}

/// Converts immutable Query Application fields to their MySQL schema form.
/// The returned columns must outlive the RowWriter opened with them.
pub fn mysql_columns_for_result_fields(fields: &[ResultField]) -> io::Result<Vec<Column>> {
    fields
        .iter()
        .map(mysql_column_for_result_field)
        .collect::<Result<Vec<_>, _>>()
        .map_err(invalid_data_error)
}

/// The wire-visible failure of opening a cancellable MySQL result. Statement
/// settlement remains an application decision at the composition boundary.
pub enum MysqlResultStartError {
    Cancelled(QueryExecutionError),
    Io(io::Error),
}

/// Opens a MySQL result while observing query cancellation before its schema
/// reaches the socket.
pub async fn start_cancellable_result<'writer, 'columns, W: AsyncWrite + Unpin>(
    results: QueryResultWriter<'writer, W>,
    columns: &'columns [Column],
    cancellation: QueryCancellationView,
) -> Result<opensrv_mysql::RowWriter<'writer, 'columns, W>, MysqlResultStartError> {
    let start = results.start(columns);
    tokio::pin!(start);
    tokio::select! {
        biased;
        reason = cancellation.cancelled() => {
            Err(MysqlResultStartError::Cancelled(cancelled_delivery(reason)))
        }
        writer = &mut start => writer.map_err(MysqlResultStartError::Io),
    }
}

/// Opens a streaming MySQL result after converting its immutable Query
/// Application schema, while observing logical failure and cancellation before
/// the schema reaches the socket.
pub async fn start_streaming_result<'writer, 'columns, W: AsyncWrite + Unpin>(
    results: QueryResultWriter<'writer, W>,
    columns: &'columns [Column],
    cancellation: QueryCancellationView,
    mut failure: ResultFailureView,
) -> Result<opensrv_mysql::RowWriter<'writer, 'columns, W>, MysqlBatchWriteError> {
    let start = results.start(columns);
    tokio::pin!(start);
    tokio::select! {
        biased;
        error = failure.wait() => Err(MysqlBatchWriteError::Native(error)),
        reason = cancellation.cancelled() => {
            Err(MysqlBatchWriteError::Cancelled(cancelled_delivery(reason)))
        }
        writer = &mut start => writer.map_err(MysqlBatchWriteError::Io),
    }
}

/// Closes an already-open MySQL result with one typed Query Application error.
/// The application caller retains responsibility for settling its delivery and
/// statement owners from the resulting socket outcome.
pub async fn finish_result_error<W: AsyncWrite + Unpin>(
    writer: opensrv_mysql::RowWriter<'_, '_, W>,
    kind: ErrorKind,
    error: &QueryExecutionError,
) -> io::Result<()> {
    let message = error.to_string().into_bytes();
    writer.finish_error(kind, &message).await
}

/// Finishes one already-open result with its success EOF.
pub async fn finish_result<W: AsyncWrite + Unpin>(
    writer: opensrv_mysql::RowWriter<'_, '_, W>,
) -> io::Result<()> {
    writer.finish().await
}

/// Finishes one result and returns the connection writer for a negotiated
/// subsequent result.
pub async fn finish_result_one<'writer, W: AsyncWrite + Unpin>(
    writer: opensrv_mysql::RowWriter<'writer, '_, W>,
) -> io::Result<QueryResultWriter<'writer, W>> {
    writer.finish_one().await
}

/// The wire-visible result of attempting a streaming success EOF.
pub enum MysqlResultFinishError {
    Native(QueryExecutionError),
    Io(io::Error),
}

/// Finishes a streaming result while keeping the logical failure observation
/// ahead of the MySQL success EOF write.
pub async fn finish_streaming_result<W: AsyncWrite + Unpin>(
    writer: opensrv_mysql::RowWriter<'_, '_, W>,
    failure: ResultFailureView,
) -> Result<(), MysqlResultFinishError> {
    match finish_streaming_result_one(writer, failure).await {
        Ok(results) => results
            .no_more_results()
            .await
            .map_err(MysqlResultFinishError::Io),
        Err(error) => Err(error),
    }
}

/// Finishes one streaming result while retaining the connection writer for a
/// negotiated following result.
pub async fn finish_streaming_result_one<'writer, W: AsyncWrite + Unpin>(
    writer: opensrv_mysql::RowWriter<'writer, '_, W>,
    mut failure: ResultFailureView,
) -> Result<QueryResultWriter<'writer, W>, MysqlResultFinishError> {
    let finish = writer.finish_one();
    tokio::pin!(finish);
    tokio::select! {
        biased;
        error = failure.wait() => Err(MysqlResultFinishError::Native(error)),
        finished = &mut finish => finished.map_err(MysqlResultFinishError::Io),
    }
}

fn invalid_data_error(error: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

/// The wire-visible failure of a cancellable row write. Statement settlement
/// remains an application decision at the composition boundary.
pub enum MysqlBatchWriteError {
    Cancelled(QueryExecutionError),
    Native(QueryExecutionError),
    Encoding(io::Error),
    Io(io::Error),
}

/// Writes a result batch while observing the Query Application cancellation
/// view before each row reaches the MySQL socket.
pub async fn write_cancellable_batch<W: AsyncWrite + Unpin>(
    writer: &mut opensrv_mysql::RowWriter<'_, '_, W>,
    batch: &RecordBatch,
    fields: &[ResultField],
    cancellation: QueryCancellationView,
) -> Result<(), MysqlBatchWriteError> {
    for row_idx in 0..batch.num_rows() {
        let row = build_mysql_row(batch, fields, row_idx)
            .map_err(invalid_data_error)
            .map_err(MysqlBatchWriteError::Encoding)?;
        let write = writer.write_row(row);
        tokio::pin!(write);
        tokio::select! {
            biased;
            reason = cancellation.cancelled() => {
                return Err(MysqlBatchWriteError::Cancelled(cancelled_delivery(reason)));
            }
            written = &mut write => written.map_err(MysqlBatchWriteError::Io)?,
        }
    }
    Ok(())
}

/// Writes one streaming result batch while observing both logical delivery
/// failure and query cancellation before each row reaches the MySQL socket.
pub async fn write_streaming_batch<W: AsyncWrite + Unpin>(
    writer: &mut opensrv_mysql::RowWriter<'_, '_, W>,
    batch: &RecordBatch,
    fields: &[ResultField],
    cancellation: QueryCancellationView,
    mut failure: ResultFailureView,
) -> Result<(), MysqlBatchWriteError> {
    for row_idx in 0..batch.num_rows() {
        let row = build_mysql_row(batch, fields, row_idx)
            .map_err(invalid_data_error)
            .map_err(MysqlBatchWriteError::Encoding)?;
        let write = writer.write_row(row);
        tokio::pin!(write);
        tokio::select! {
            biased;
            error = failure.wait() => {
                return Err(MysqlBatchWriteError::Native(error));
            }
            reason = cancellation.cancelled() => {
                return Err(MysqlBatchWriteError::Cancelled(cancelled_delivery(reason)));
            }
            written = &mut write => written.map_err(MysqlBatchWriteError::Io)?,
        }
    }
    Ok(())
}

fn cancelled_delivery(reason: QueryCancellationReason) -> QueryExecutionError {
    QueryExecutionError::new(
        QueryExecutionErrorKind::Cancelled,
        format!("MySQL result delivery cancelled: {reason:?}"),
    )
}
