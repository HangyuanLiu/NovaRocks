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
use novarocks_query_application::api::ResultField;
use opensrv_mysql::QueryResultWriter;
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
    let columns = fields
        .iter()
        .map(mysql_column_for_result_field)
        .collect::<Result<Vec<_>, _>>()
        .map_err(invalid_data_error)?;
    let mut writer = results.start(columns.as_slice()).await?;
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            writer
                .write_row(build_mysql_row(batch, fields, row_idx).map_err(invalid_data_error)?)
                .await?;
        }
    }
    writer.finish().await
}

fn invalid_data_error(error: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}
