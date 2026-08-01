use std::collections::VecDeque;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorBatchReader, ConnectorError, ConnectorErrorKind, ConnectorOpenReaderRequest,
    ConnectorReaderMetricsSnapshot, ConnectorRequestContext,
};

use super::codec::StarRocksRpcSplit;

pub trait StarRocksArrowFlightStream: Send {
    fn next_batch(
        &mut self,
        context: &ConnectorRequestContext,
    ) -> Result<Option<RecordBatch>, ConnectorError>;
    fn close(&mut self) -> Result<(), ConnectorError>;
}

pub trait StarRocksArrowFlightClient: Send + Sync {
    fn open(
        &self,
        split: &StarRocksRpcSplit,
        ticket: Bytes,
        context: &ConnectorRequestContext,
    ) -> Result<Box<dyn StarRocksArrowFlightStream>, ConnectorError>;
}

pub struct StarRocksFlightReader {
    stream: Box<dyn StarRocksArrowFlightStream>,
    expected: arrow::datatypes::SchemaRef,
    pending: VecDeque<RecordBatch>,
    request: ConnectorOpenReaderRequest,
    closed: bool,
    metrics: ConnectorReaderMetricsSnapshot,
}

impl StarRocksFlightReader {
    pub fn open(
        client: Arc<dyn StarRocksArrowFlightClient>,
        split: StarRocksRpcSplit,
        request: ConnectorOpenReaderRequest,
    ) -> Result<Self, ConnectorError> {
        if request.context.cancellation().is_cancelled() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Cancelled,
                "connector request was cancelled",
            ));
        }
        let token = std::str::from_utf8(split.token()).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "StarRocks Arrow Flight split token is not valid UTF-8",
            )
        })?;
        let ticket = Bytes::from(format!("remote_scan:{token}"));
        let stream = client.open(&split, ticket, &request.context)?;
        Ok(Self {
            stream,
            expected: request.expected_schema.clone(),
            pending: VecDeque::new(),
            request,
            closed: false,
            metrics: ConnectorReaderMetricsSnapshot::default(),
        })
    }
}

impl ConnectorBatchReader for StarRocksFlightReader {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        if self.closed {
            return Ok(None);
        }
        if self.request.context.cancellation().is_cancelled() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Cancelled,
                "connector request was cancelled",
            ));
        }
        if std::time::Instant::now() >= self.request.context.deadline() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::DeadlineExceeded,
                "connector request deadline elapsed",
            ));
        }
        if let Some(batch) = self.pending.pop_front() {
            return Ok(Some(batch));
        }
        let Some(batch) = self.stream.next_batch(&self.request.context)? else {
            self.closed = true;
            return Ok(None);
        };
        if batch.schema() != self.expected {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "Arrow Flight batch schema does not match frozen split schema",
            ));
        }
        if batch.num_rows() > self.request.batch.max_rows.get() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "Arrow Flight batch exceeds row budget",
            ));
        }
        if batch.get_array_memory_size() > self.request.batch.max_bytes.get() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "Arrow Flight batch exceeds byte budget",
            ));
        }
        self.metrics.batches_delivered += 1;
        self.metrics.rows_decoded += batch.num_rows() as u64;
        Ok(Some(batch))
    }
    fn close(&mut self) -> Result<(), ConnectorError> {
        if !self.closed {
            self.closed = true;
            self.stream.close()?;
        }
        Ok(())
    }
    fn metrics_snapshot(&self) -> ConnectorReaderMetricsSnapshot {
        self.metrics
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::time::{Duration, Instant};

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use novarocks_spi::connector::{
        ConnectorBatchBudget, ConnectorCancellation, ConnectorOpenReaderRequest,
    };

    use super::*;

    struct NeverCancelled;
    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    struct Stream(Option<RecordBatch>);
    impl StarRocksArrowFlightStream for Stream {
        fn next_batch(
            &mut self,
            _: &ConnectorRequestContext,
        ) -> Result<Option<RecordBatch>, ConnectorError> {
            Ok(self.0.take())
        }
        fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }
    }

    struct Client;
    impl StarRocksArrowFlightClient for Client {
        fn open(
            &self,
            _: &StarRocksRpcSplit,
            ticket: Bytes,
            _: &ConnectorRequestContext,
        ) -> Result<Box<dyn StarRocksArrowFlightStream>, ConnectorError> {
            assert!(ticket.starts_with(b"remote_scan:"));
            Ok(Box::new(Stream(Some(
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new(
                        "value",
                        DataType::Int64,
                        false,
                    )])),
                    vec![Arc::new(Int64Array::from(vec![8_i64]))],
                )
                .expect("batch"),
            ))))
        }
    }

    fn request() -> ConnectorOpenReaderRequest {
        ConnectorOpenReaderRequest {
            expected_schema: Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )])),
            batch: ConnectorBatchBudget {
                max_rows: NonZeroUsize::new(8).expect("rows"),
                max_bytes: NonZeroUsize::new(4096).expect("bytes"),
            },
            context: ConnectorRequestContext::try_new(
                Instant::now() + Duration::from_secs(5),
                Arc::new(NeverCancelled),
                4096,
                4096,
            )
            .expect("context"),
        }
    }

    fn split() -> StarRocksRpcSplit {
        StarRocksRpcSplit::try_new(
            crate::domain::StarRocksRpcTransport::ArrowFlight,
            crate::rpc::StarRocksRemoteEndpoint::try_new("be.example", 8040).expect("endpoint"),
            Bytes::from_static(b"secret-token"),
            vec![crate::rpc::StarRocksRpcOutputBinding {
                output_index: Some(0),
                remote_slot_id: 1,
                name: Arc::from("value"),
                data_type: DataType::Int64,
                nullable: false,
                is_const: false,
                row_marker: false,
            }],
        )
        .expect("split")
    }

    #[test]
    fn starrocks_flight_reader_delivers_only_the_flight_batch() {
        let mut reader =
            StarRocksFlightReader::open(Arc::new(Client), split(), request()).expect("open");
        assert_eq!(
            reader
                .next_batch()
                .expect("batch")
                .expect("some")
                .num_rows(),
            1
        );
        assert!(reader.next_batch().expect("eos").is_none());
        reader.close().expect("idempotent close");
    }
}
