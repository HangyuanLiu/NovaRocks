// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the License
// at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::{Arc, Mutex};

use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_spi::connector::{ConnectorBatchReader, ConnectorError, ConnectorErrorKind};

use super::runtime::ConnectorBatchReaderIter;
use crate::common::ids::SlotId;
use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};

struct FakeReader {
    batches: Vec<Result<Option<RecordBatch>, ConnectorError>>,
    close_result: Result<(), ConnectorError>,
    close_calls: Arc<Mutex<usize>>,
}

impl ConnectorBatchReader for FakeReader {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        self.batches.remove(0)
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        *self.close_calls.lock().expect("close calls") += 1;
        self.close_result.clone()
    }
}

fn chunk_schema() -> Arc<ChunkSchema> {
    Arc::new(
        ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
            SlotId::new(1),
            Field::new("id", DataType::Int32, false),
            None,
            None,
        )])
        .expect("chunk schema"),
    )
}

fn batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![7]))],
    )
    .expect("record batch")
}

#[test]
fn reader_iterator_converts_batches_and_closes_once_at_eos() {
    let close_calls = Arc::new(Mutex::new(0));
    let reader = FakeReader {
        batches: vec![Ok(Some(batch())), Ok(None)],
        close_result: Ok(()),
        close_calls: Arc::clone(&close_calls),
    };
    let chunks = ConnectorBatchReaderIter::new(Box::new(reader), chunk_schema())
        .collect::<Result<Vec<_>, _>>()
        .expect("reader chunks");
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].len(), 1);
    assert_eq!(*close_calls.lock().expect("close calls"), 1);
}

#[test]
fn reader_iterator_preserves_primary_read_failure_and_cleanup_context() {
    let close_calls = Arc::new(Mutex::new(0));
    let reader = FakeReader {
        batches: vec![Err(ConnectorError::new(
            ConnectorErrorKind::Unavailable,
            "primary read failure",
        ))],
        close_result: Err(ConnectorError::new(
            ConnectorErrorKind::Internal,
            "cleanup failure",
        )),
        close_calls: Arc::clone(&close_calls),
    };
    let err = ConnectorBatchReaderIter::new(Box::new(reader), chunk_schema())
        .next()
        .expect("reader result")
        .expect_err("reader must fail");
    assert!(err.contains("primary read failure"), "err={err}");
    assert!(err.contains("cleanup failure"), "err={err}");
    assert_eq!(*close_calls.lock().expect("close calls"), 1);
}
