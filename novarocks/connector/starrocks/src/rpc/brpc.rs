use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, StringArray,
};
use arrow::record_batch::RecordBatch;
use bytes::{Buf, Bytes};
use novarocks_spi::connector::{
    ConnectorBatchReader, ConnectorError, ConnectorErrorKind, ConnectorOpenReaderRequest,
    ConnectorReaderMetricsSnapshot,
};
use prost::Message;

use super::codec::StarRocksRpcOutputBinding;
use super::{StarRocksBrpcTransport, StarRocksRpcSplit};

#[derive(Clone, PartialEq, Message)]
struct FetchRequest {
    #[prost(string, optional, tag = "1")]
    scan_token: Option<String>,
    #[prost(int64, optional, tag = "2")]
    packet_seq: Option<i64>,
}
#[derive(Clone, PartialEq, Message)]
struct StatusPb {
    #[prost(int32, optional, tag = "1")]
    status_code: Option<i32>,
    #[prost(string, optional, tag = "2")]
    error_msgs: Option<String>,
}
#[derive(Clone, PartialEq, Message)]
struct ChunkPb {
    #[prost(bytes = "vec", optional, tag = "1")]
    data: Option<Vec<u8>>,
    #[prost(int32, optional, tag = "2")]
    compress_type: Option<i32>,
    #[prost(int64, optional, tag = "9")]
    serialized_size: Option<i64>,
    #[prost(int32, repeated, tag = "4")]
    slot_id_map: Vec<i32>,
    #[prost(bool, repeated, tag = "5")]
    is_nulls: Vec<bool>,
    #[prost(bool, repeated, tag = "6")]
    is_consts: Vec<bool>,
    #[prost(int32, repeated, tag = "10")]
    encode_level: Vec<i32>,
}
#[derive(Clone, PartialEq, Message)]
struct FetchResult {
    #[prost(message, optional, tag = "1")]
    status: Option<StatusPb>,
    #[prost(bool, optional, tag = "2")]
    eos: Option<bool>,
    #[prost(int64, optional, tag = "3")]
    packet_seq: Option<i64>,
    #[prost(message, optional, tag = "4")]
    chunk: Option<ChunkPb>,
}

pub struct StarRocksBrpcReader {
    transport: Arc<dyn StarRocksBrpcTransport>,
    split: StarRocksRpcSplit,
    request: ConnectorOpenReaderRequest,
    packet_seq: i64,
    closed: bool,
    pending: VecDeque<RecordBatch>,
    metrics: ConnectorReaderMetricsSnapshot,
}
impl StarRocksBrpcReader {
    pub fn open(
        transport: Arc<dyn StarRocksBrpcTransport>,
        split: StarRocksRpcSplit,
        request: ConnectorOpenReaderRequest,
    ) -> Self {
        Self {
            transport,
            split,
            request,
            packet_seq: 0,
            closed: false,
            pending: VecDeque::new(),
            metrics: ConnectorReaderMetricsSnapshot::default(),
        }
    }
    fn fetch(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        active(&self.request)?;
        let scan_token = std::str::from_utf8(self.split.token()).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "StarRocks BRPC split token is not valid UTF-8",
            )
        })?;
        let request = FetchRequest {
            scan_token: Some(scan_token.to_owned()),
            packet_seq: Some(self.packet_seq),
        }
        .encode_to_vec();
        let bytes = self.transport.fetch(
            self.split.endpoint(),
            Bytes::from(request),
            &self.request.context,
        )?;
        self.metrics.read_requests += 1;
        self.metrics.bytes_read += bytes.len() as u64;
        validate_fetch_result_wire(&bytes)?;
        let response = FetchResult::decode(bytes).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "decode StarRocks BRPC fetch response",
            )
        })?;
        let status = response.status.ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "StarRocks BRPC response is missing status",
            )
        })?;
        if status.status_code.unwrap_or(-1) != 0 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unavailable,
                "StarRocks BRPC remote status failed",
            ));
        }
        if response.packet_seq != Some(self.packet_seq) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "StarRocks BRPC response packet sequence mismatch",
            ));
        }
        if response.eos.unwrap_or(false) {
            if response.chunk.is_some() {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "StarRocks BRPC EOS response includes a chunk",
                ));
            }
            self.closed = true;
            return Ok(None);
        }
        let chunk = response.chunk.ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "StarRocks BRPC response is missing chunk",
            )
        })?;
        let batch = decode_chunk(&chunk, &self.split, self.request.expected_schema.clone())?;
        self.packet_seq += 1;
        self.metrics.rows_decoded += batch.num_rows() as u64;
        self.metrics.batches_delivered += 1;
        if batch.num_rows() > self.request.batch.max_rows.get()
            || batch.get_array_memory_size() > self.request.batch.max_bytes.get()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "StarRocks BRPC batch exceeds read budget",
            ));
        }
        Ok(Some(batch))
    }
}
impl ConnectorBatchReader for StarRocksBrpcReader {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        if self.closed {
            return Ok(None);
        }
        if let Some(batch) = self.pending.pop_front() {
            return Ok(Some(batch));
        }
        self.fetch()
    }
    fn close(&mut self) -> Result<(), ConnectorError> {
        self.closed = true;
        self.pending.clear();
        Ok(())
    }
    fn metrics_snapshot(&self) -> ConnectorReaderMetricsSnapshot {
        self.metrics
    }
}

fn active(request: &ConnectorOpenReaderRequest) -> Result<(), ConnectorError> {
    if request.context.cancellation().is_cancelled() {
        Err(ConnectorError::new(
            ConnectorErrorKind::Cancelled,
            "connector request was cancelled",
        ))
    } else if std::time::Instant::now() >= request.context.deadline() {
        Err(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "connector request deadline elapsed",
        ))
    } else {
        Ok(())
    }
}
fn decode_chunk(
    chunk: &ChunkPb,
    split: &StarRocksRpcSplit,
    expected: arrow::datatypes::SchemaRef,
) -> Result<RecordBatch, ConnectorError> {
    if chunk.compress_type.unwrap_or(2) != 2 || chunk.encode_level.iter().any(|value| *value != 0) {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "unsupported StarRocks BRPC chunk encoding",
        ));
    }
    let data = chunk.data.as_ref().ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "StarRocks BRPC chunk has no data",
        )
    })?;
    if chunk
        .serialized_size
        .is_some_and(|size| size < 0 || size as usize != data.len())
        || chunk.slot_id_map
            != split
                .outputs()
                .iter()
                .map(|output| output.remote_slot_id)
                .collect::<Vec<_>>()
        || chunk.is_nulls.len() != split.outputs().len()
        || chunk.is_consts.len() != split.outputs().len()
        || (!chunk.encode_level.is_empty() && chunk.encode_level.len() != split.outputs().len())
        || chunk
            .is_nulls
            .iter()
            .zip(split.outputs())
            .any(|(actual, expected)| *actual != expected.nullable)
        || chunk
            .is_consts
            .iter()
            .zip(split.outputs())
            .any(|(actual, expected)| *actual != expected.is_const)
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "StarRocks BRPC chunk metadata does not match the frozen RPC split",
        ));
    }
    let mut cursor = Bytes::copy_from_slice(data);
    if cursor.remaining() < 8 || cursor.get_u32_le() != 1 {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "invalid StarRocks BRPC chunk header",
        ));
    }
    let rows = cursor.get_u32_le() as usize;
    let mut arrays = Vec::new();
    for output in split.outputs() {
        let values = decode_values(&mut cursor, output, rows)?;
        if output.output_index.is_some() {
            arrays.push(array(output, values)?);
        }
    }
    if cursor.has_remaining() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "StarRocks BRPC chunk has trailing data",
        ));
    }
    let batch = RecordBatch::try_new_with_options(
        expected,
        arrays,
        &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .map_err(|_| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "StarRocks BRPC chunk schema mismatch",
        )
    })?;
    Ok(batch)
}
#[derive(Clone)]
enum Value {
    Null,
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    String(String),
    Binary(Vec<u8>),
}
fn decode_values(
    cursor: &mut Bytes,
    binding: &StarRocksRpcOutputBinding,
    rows: usize,
) -> Result<Vec<Value>, ConnectorError> {
    let nulls = if binding.nullable {
        fixed(cursor, rows)?
            .into_iter()
            .map(|value| value != 0)
            .collect::<Vec<_>>()
    } else {
        vec![false; rows]
    };
    let logical = if binding.is_const {
        if cursor.remaining() < 8 {
            return corrupt();
        }
        cursor.get_u64_le() as usize
    } else {
        rows
    };
    if logical != rows {
        return corrupt();
    }
    let count = if binding.is_const { 1 } else { rows };
    let mut values = raw_values(cursor, &binding.data_type, count)?;
    if binding.is_const {
        values = std::iter::repeat(values.into_iter().next().ok_or_else(|| {
            ConnectorError::new(ConnectorErrorKind::CorruptData, "empty constant column")
        })?)
        .take(rows)
        .collect();
    }
    for (value, null) in values.iter_mut().zip(nulls) {
        if null {
            *value = Value::Null;
        }
    }
    Ok(values)
}
fn raw_values(
    cursor: &mut Bytes,
    data_type: &arrow::datatypes::DataType,
    rows: usize,
) -> Result<Vec<Value>, ConnectorError> {
    match data_type {
        arrow::datatypes::DataType::Boolean => Ok(fixed(cursor, rows)?
            .into_iter()
            .map(|v| Value::Bool(v != 0))
            .collect()),
        arrow::datatypes::DataType::Int8 => Ok(fixed(cursor, rows)?
            .into_iter()
            .map(|v| Value::I8(v as i8))
            .collect()),
        arrow::datatypes::DataType::Int16 => fixed_width(cursor, rows, 2, |b| {
            Value::I16(i16::from_le_bytes([b[0], b[1]]))
        }),
        arrow::datatypes::DataType::Int32 => fixed_width(cursor, rows, 4, |b| {
            Value::I32(i32::from_le_bytes([b[0], b[1], b[2], b[3]]))
        }),
        arrow::datatypes::DataType::Int64 => fixed_width(cursor, rows, 8, |b| {
            Value::I64(i64::from_le_bytes(b.try_into().unwrap()))
        }),
        arrow::datatypes::DataType::Float32 => fixed_width(cursor, rows, 4, |b| {
            Value::F32(f32::from_le_bytes([b[0], b[1], b[2], b[3]]))
        }),
        arrow::datatypes::DataType::Float64 => fixed_width(cursor, rows, 8, |b| {
            Value::F64(f64::from_le_bytes(b.try_into().unwrap()))
        }),
        arrow::datatypes::DataType::Utf8 | arrow::datatypes::DataType::Binary => {
            if cursor.remaining() < 4 {
                return corrupt();
            }
            let size = cursor.get_u32_le() as usize;
            if cursor.remaining() < size + 4 {
                return corrupt();
            }
            let bytes = cursor.split_to(size);
            let offsets_size = cursor.get_u32_le() as usize;
            if offsets_size != (rows + 1) * 4 || cursor.remaining() < offsets_size {
                return corrupt();
            }
            let offsets = (0..=rows)
                .map(|_| cursor.get_u32_le() as usize)
                .collect::<Vec<_>>();
            if offsets.first() != Some(&0)
                || offsets.last() != Some(&size)
                || offsets.windows(2).any(|pair| pair[0] > pair[1])
            {
                return corrupt();
            }
            Ok(offsets
                .windows(2)
                .map(|pair| {
                    let value = bytes.slice(pair[0]..pair[1]);
                    if matches!(data_type, arrow::datatypes::DataType::Utf8) {
                        std::str::from_utf8(&value)
                            .map(|v| Value::String(v.to_string()))
                            .map_err(|_| {
                                ConnectorError::new(
                                    ConnectorErrorKind::CorruptData,
                                    "invalid StarRocks string",
                                )
                            })
                    } else {
                        Ok(Value::Binary(value.to_vec()))
                    }
                })
                .collect::<Result<Vec<_>, _>>()?)
        }
        _ => Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "unsupported StarRocks BRPC output type",
        )),
    }
}
fn fixed(cursor: &mut Bytes, rows: usize) -> Result<Vec<u8>, ConnectorError> {
    if cursor.remaining() < 4 {
        return corrupt();
    }
    let size = cursor.get_u32_le() as usize;
    if size != rows || cursor.remaining() < size {
        return corrupt();
    }
    Ok(cursor.split_to(size).to_vec())
}
fn fixed_width<F: Fn(&[u8]) -> Value>(
    cursor: &mut Bytes,
    rows: usize,
    width: usize,
    map: F,
) -> Result<Vec<Value>, ConnectorError> {
    if cursor.remaining() < 4 {
        return corrupt();
    }
    let size = cursor.get_u32_le() as usize;
    if size != rows * width || cursor.remaining() < size {
        return corrupt();
    }
    let data = cursor.split_to(size);
    Ok(data.chunks_exact(width).map(|v| map(v)).collect())
}
fn array(
    binding: &StarRocksRpcOutputBinding,
    values: Vec<Value>,
) -> Result<ArrayRef, ConnectorError> {
    macro_rules! vals {
        ($variant:ident,$array:ident) => {
            Ok(Arc::new($array::from(
                values
                    .into_iter()
                    .map(|v| match v {
                        Value::Null => None,
                        Value::$variant(v) => Some(v),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )) as ArrayRef)
        };
    }
    match binding.data_type {
        arrow::datatypes::DataType::Boolean => vals!(Bool, BooleanArray),
        arrow::datatypes::DataType::Int8 => vals!(I8, Int8Array),
        arrow::datatypes::DataType::Int16 => vals!(I16, Int16Array),
        arrow::datatypes::DataType::Int32 => vals!(I32, Int32Array),
        arrow::datatypes::DataType::Int64 => vals!(I64, Int64Array),
        arrow::datatypes::DataType::Float32 => vals!(F32, Float32Array),
        arrow::datatypes::DataType::Float64 => vals!(F64, Float64Array),
        arrow::datatypes::DataType::Utf8 => Ok(Arc::new(StringArray::from(
            values
                .into_iter()
                .map(|v| match v {
                    Value::Null => None,
                    Value::String(v) => Some(v),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        arrow::datatypes::DataType::Binary => {
            let values = values
                .iter()
                .map(|v| match v {
                    Value::Null => None,
                    Value::Binary(v) => Some(v.as_slice()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(BinaryArray::from(values)))
        }
        _ => Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "unsupported StarRocks BRPC output type",
        )),
    }
}
fn corrupt<T>() -> Result<T, ConnectorError> {
    Err(ConnectorError::new(
        ConnectorErrorKind::CorruptData,
        "malformed StarRocks ColumnArraySerde payload",
    ))
}

/// Prost deliberately preserves unknown protobuf fields. The remote-scan
/// snapshot is closed, so reject them before the generated DTO is decoded.
fn validate_fetch_result_wire(bytes: &[u8]) -> Result<(), ConnectorError> {
    let fields = wire_fields(bytes, &[1, 2, 3, 4])?;
    for (field, payload) in fields {
        match field {
            1 => {
                wire_fields(payload, &[1, 2])?;
            }
            4 => {
                wire_fields(payload, &[1, 2, 4, 5, 6, 9, 10])?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn wire_fields<'a>(
    bytes: &'a [u8],
    allowed: &[u32],
) -> Result<Vec<(u32, &'a [u8])>, ConnectorError> {
    let mut cursor = 0;
    let mut fields = Vec::new();
    while cursor < bytes.len() {
        let key = read_varint(bytes, &mut cursor)?;
        let field = (key >> 3) as u32;
        if field == 0 || !allowed.contains(&field) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "StarRocks remote protobuf contains an unknown field",
            ));
        }
        match key & 7 {
            0 => {
                read_varint(bytes, &mut cursor)?;
            }
            1 => {
                cursor = cursor
                    .checked_add(8)
                    .filter(|end| *end <= bytes.len())
                    .ok_or_else(|| {
                        ConnectorError::new(
                            ConnectorErrorKind::CorruptData,
                            "truncated StarRocks remote protobuf",
                        )
                    })?;
            }
            2 => {
                let length = usize::try_from(read_varint(bytes, &mut cursor)?).map_err(|_| {
                    ConnectorError::new(
                        ConnectorErrorKind::CorruptData,
                        "invalid StarRocks remote protobuf length",
                    )
                })?;
                let end = cursor
                    .checked_add(length)
                    .filter(|end| *end <= bytes.len())
                    .ok_or_else(|| {
                        ConnectorError::new(
                            ConnectorErrorKind::CorruptData,
                            "truncated StarRocks remote protobuf",
                        )
                    })?;
                fields.push((field, &bytes[cursor..end]));
                cursor = end;
            }
            5 => {
                cursor = cursor
                    .checked_add(4)
                    .filter(|end| *end <= bytes.len())
                    .ok_or_else(|| {
                        ConnectorError::new(
                            ConnectorErrorKind::CorruptData,
                            "truncated StarRocks remote protobuf",
                        )
                    })?;
            }
            _ => {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "unsupported StarRocks remote protobuf wire type",
                ));
            }
        }
    }
    Ok(fields)
}

fn read_varint(bytes: &[u8], cursor: &mut usize) -> Result<u64, ConnectorError> {
    let mut value = 0_u64;
    for shift in (0..64).step_by(7) {
        let byte = *bytes.get(*cursor).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "truncated StarRocks remote protobuf",
            )
        })?;
        *cursor += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(ConnectorError::new(
        ConnectorErrorKind::CorruptData,
        "invalid StarRocks remote protobuf varint",
    ))
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn split() -> StarRocksRpcSplit {
        StarRocksRpcSplit::try_new(
            crate::domain::StarRocksRpcTransport::BrpcChunk,
            crate::rpc::StarRocksRemoteEndpoint::try_new("be.example", 8040).expect("endpoint"),
            Bytes::from_static(b"secret-token"),
            vec![StarRocksRpcOutputBinding {
                output_index: Some(0),
                remote_slot_id: 7,
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
    fn starrocks_brpc_reader_decodes_v1_int64_fixture() {
        let mut data = Vec::new();
        data.extend_from_slice(&1_u32.to_le_bytes());
        data.extend_from_slice(&2_u32.to_le_bytes());
        data.extend_from_slice(&16_u32.to_le_bytes());
        data.extend_from_slice(&4_i64.to_le_bytes());
        data.extend_from_slice(&9_i64.to_le_bytes());
        let chunk = ChunkPb {
            data: Some(data),
            compress_type: Some(2),
            serialized_size: None,
            slot_id_map: vec![7],
            is_nulls: vec![false],
            is_consts: vec![false],
            encode_level: vec![0],
        };

        let batch = decode_chunk(
            &chunk,
            &split(),
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )])),
        )
        .expect("fixture decodes");

        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 output");
        assert_eq!(values.values(), &[4, 9]);
    }

    #[test]
    fn starrocks_brpc_reader_rejects_unsupported_compression() {
        let error = decode_chunk(
            &ChunkPb {
                data: Some(vec![1, 0, 0, 0, 0, 0, 0, 0]),
                compress_type: Some(1),
                serialized_size: None,
                slot_id_map: vec![],
                is_nulls: vec![],
                is_consts: vec![],
                encode_level: vec![],
            },
            &split(),
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )])),
        )
        .expect_err("compression is deliberately unsupported");
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
    }
}
