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

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use super::{ConnectorBatchBudget, ConnectorBatchReader, ConnectorError, ConnectorErrorKind};

pub fn assert_batch_reader_contract(
    reader: &mut dyn ConnectorBatchReader,
    expected_schema: &SchemaRef,
    budget: ConnectorBatchBudget,
) -> Result<Vec<RecordBatch>, ConnectorError> {
    let result = read_batches(reader, expected_schema, budget);
    let close_error = close_idempotently(reader).err();
    match (result, close_error) {
        (Ok(batches), None) => Ok(batches),
        (Ok(_), Some(error)) => Err(error),
        (Err(primary), None) => Err(primary),
        (Err(primary), Some(cleanup)) => Err(primary.with_cleanup_context(cleanup.to_string())),
    }
}

fn read_batches(
    reader: &mut dyn ConnectorBatchReader,
    expected_schema: &SchemaRef,
    budget: ConnectorBatchBudget,
) -> Result<Vec<RecordBatch>, ConnectorError> {
    let mut batches = Vec::new();
    while let Some(batch) = reader.next_batch()? {
        validate_batch(&batch, expected_schema, budget)?;
        batches.push(batch);
    }
    if reader.next_batch()?.is_some() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "connector reader returned a batch after end of stream",
        ));
    }
    Ok(batches)
}

fn validate_batch(
    batch: &RecordBatch,
    expected_schema: &SchemaRef,
    budget: ConnectorBatchBudget,
) -> Result<(), ConnectorError> {
    if batch.schema().as_ref() != expected_schema.as_ref() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "connector reader batch schema differs from its declared output schema",
        ));
    }
    if batch.num_rows() > budget.max_rows.get() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "connector reader batch exceeds the row budget",
        ));
    }
    if batch.get_array_memory_size() > budget.max_bytes.get() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "connector reader batch exceeds the byte budget",
        ));
    }
    Ok(())
}

fn close_idempotently(reader: &mut dyn ConnectorBatchReader) -> Result<(), ConnectorError> {
    reader.close()?;
    reader.close()
}
