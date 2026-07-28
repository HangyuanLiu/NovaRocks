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
use novarocks_spi::connector::{
    ConnectorBatchBudget, ConnectorBatchReader, ConnectorBeginScanRequest, ConnectorError,
    ConnectorErrorKind, ConnectorInstance, ConnectorInstanceDescriptor, ConnectorInstanceId,
    ConnectorOpenReaderRequest, ConnectorProviderId, ConnectorRead, ConnectorReadSelector,
    ConnectorRequestContext, ConnectorScan, ConnectorScanHandle, ConnectorSplit,
    ConnectorSplitPlanningRequest, ConnectorTableHandle,
};

use super::runtime::{ConnectorBatchReaderIter, ConnectorReadScanSource};
use crate::common::ids::SlotId;
use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
use crate::exec::node::scan::{BoundScanRanges, ScanMorsel, ScanOp, ScanSource};

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

struct FakeRead {
    instance_id: ConnectorInstanceId,
}

impl ConnectorRead for FakeRead {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }

    fn begin_scan(
        &self,
        _table: &ConnectorTableHandle,
        _request: ConnectorBeginScanRequest,
    ) -> Result<ConnectorScan, ConnectorError> {
        unreachable!("runtime source starts after split planning")
    }

    fn plan_splits(
        &self,
        _scan: &ConnectorScanHandle,
        _request: ConnectorSplitPlanningRequest,
    ) -> Result<Vec<ConnectorSplit>, ConnectorError> {
        unreachable!("runtime source starts after split planning")
    }

    fn open_reader(
        &self,
        _split: &ConnectorSplit,
        _request: ConnectorOpenReaderRequest,
    ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
        Ok(Box::new(FakeReader {
            batches: vec![Ok(Some(batch())), Ok(None)],
            close_result: Ok(()),
            close_calls: Arc::new(Mutex::new(0)),
        }))
    }
}

fn request_context() -> ConnectorRequestContext {
    struct NotCancelled;
    impl novarocks_spi::connector::ConnectorCancellation for NotCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }
    ConnectorRequestContext::try_new(
        std::time::Instant::now() + std::time::Duration::from_secs(30),
        Arc::new(NotCancelled),
        1024,
        4096,
    )
    .expect("request context")
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

#[test]
fn read_scan_source_opens_a_typed_split_and_adapts_its_batches() {
    let instance_id = ConnectorInstanceId::parse("test").expect("instance ID");
    let read = Arc::new(FakeRead {
        instance_id: instance_id.clone(),
    });
    let instance = Arc::new(
        ConnectorInstance::try_new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("test").expect("provider ID"),
                instance_id: instance_id.clone(),
            },
            None,
            read,
        )
        .expect("connector instance"),
    );
    let split =
        ConnectorSplit::try_new(instance_id, "split", bytes::Bytes::new(), Some(1)).expect("split");
    let source = ConnectorReadScanSource::new(
        instance,
        vec![split.clone(), split],
        ConnectorOpenReaderRequest {
            expected_schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            batch: ConnectorBatchBudget {
                max_rows: std::num::NonZeroUsize::new(128).expect("rows"),
                max_bytes: std::num::NonZeroUsize::new(1024).expect("bytes"),
            },
            context: request_context(),
        },
        chunk_schema(),
    );
    let op = source.bind(BoundScanRanges::None).expect("bind source");
    let morsels = op.build_morsels().expect("build connector morsels");
    assert!(matches!(
        morsels.morsels.as_slice(),
        [
            ScanMorsel::ConnectorSplit { index: 0 },
            ScanMorsel::ConnectorSplit { index: 1 }
        ]
    ));
    let chunks = op
        .execute_iter(ScanMorsel::ConnectorSplit { index: 0 }, None, None)
        .expect("execute reader")
        .collect::<Result<Vec<_>, _>>()
        .expect("reader chunks");
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].len(), 1);
}
