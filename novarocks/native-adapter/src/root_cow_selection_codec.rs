// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Private CowSelectionArrowV1 record codec, independent of root transport.
//!
//! A copy-on-write selection is ordinary Arrow columns whose exact types the
//! frontend casts to its own signed layout. The stream is one SCHEMA record,
//! emitted with the first input, followed by one BATCH record per non-empty
//! input. Every record starts with a 32-byte header: ASCII `CSA1`, u8 kind
//! (1 SCHEMA, 2 BATCH), three zero bytes, u64LE complete record bytes
//! (INCLUDING the header), u64LE rows (BATCH; 0 for SCHEMA), u32LE type-node
//! count and u32LE buffer count (0 for SCHEMA).
//!
//! SCHEMA payload: each top-level field's type tree in pre-order, every node
//! as u8 tag, u8 nullable, u32LE name length, UTF-8 name, tag parameters, then
//! its children. Field metadata is not carried: the consumer applies its own
//! frozen layout and casts.
//!
//! BATCH payload: u64LE length of every type node (pre-order), u64LE byte
//! length of every buffer, then the buffers back to back with no padding.
//! Each node's first buffer is its validity bitmap (empty when the source
//! has no null buffer); bitmaps start at bit 0, offsets start at 0, and a
//! dictionary keeps its keys and its complete values child, so nothing is
//! expanded. Buffers are streamed straight from the source arrays.
//!
//! The closed type set has no Map, union, view or interval carrier; such a
//! selection is refused at construction. Record and batch-count limits are
//! checked before the first byte of a record. Each step examines at most
//! 64KiB and performs at most 1024 work operations. Construction allocates
//! only the node/buffer tables and the record prefix, whose exact capacity
//! the caller prepays ([`CowSelectionEncoder::scratch_bytes`]).

use std::sync::Arc;

use arrow::array::{
    Array, ArrayData, ArrayRef, AsArray, RecordBatch, RecordBatchOptions, make_array,
};
use arrow::buffer::{Buffer, NullBuffer};
use arrow::datatypes::{
    DataType, Date32Type, Date64Type, Decimal128Type, Decimal256Type, Field, Fields, Float32Type,
    Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, Schema, SchemaRef,
    Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimeUnit,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use novarocks_result_contract::RootProfileV1;
use novarocks_result_render::{RenderTurn, RenderTurnStatus};
use novarocks_spi::connector::MAX_CONNECTOR_ROW_MUTATION_SELECTION_BATCHES;

pub const COW_SELECTION_HEADER_BYTES: usize = 32;
pub const COW_SELECTION_TURN_BYTES: usize = 64 * 1024;
pub const COW_SELECTION_TURN_WORK: usize = 1024;
/// Type nodes one record may describe; it bounds schema and directory work.
pub const COW_SELECTION_MAX_NODES: usize = 2 * RootProfileV1::SCHEMA_TYPE_NODES;
/// One record never declares more than this many bytes.
pub const COW_SELECTION_MAX_RECORD_BYTES: u64 = 256 * 1024 * 1024;
const MAGIC: [u8; 4] = *b"CSA1";
const KIND_SCHEMA: u8 = 1;
const KIND_BATCH: u8 = 2;

mod tag {
    pub const NULL: u8 = 0;
    pub const BOOLEAN: u8 = 1;
    pub const INT8: u8 = 2;
    pub const INT16: u8 = 3;
    pub const INT32: u8 = 4;
    pub const INT64: u8 = 5;
    pub const UINT8: u8 = 6;
    pub const UINT16: u8 = 7;
    pub const UINT32: u8 = 8;
    pub const UINT64: u8 = 9;
    pub const FLOAT32: u8 = 10;
    pub const FLOAT64: u8 = 11;
    pub const DECIMAL128: u8 = 12;
    pub const DECIMAL256: u8 = 13;
    pub const DATE32: u8 = 14;
    pub const DATE64: u8 = 15;
    pub const TIME32: u8 = 16;
    pub const TIME64: u8 = 17;
    pub const TIMESTAMP: u8 = 18;
    pub const FIXED_SIZE_BINARY: u8 = 19;
    pub const UTF8: u8 = 20;
    pub const LARGE_UTF8: u8 = 21;
    pub const BINARY: u8 = 22;
    pub const LARGE_BINARY: u8 = 23;
    pub const LIST: u8 = 24;
    pub const LARGE_LIST: u8 = 25;
    pub const STRUCT: u8 = 26;
    pub const DICTIONARY: u8 = 27;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CowSelectionCodecError {
    UnsupportedType,
    SchemaLimit,
    SchemaChanged,
    BatchLimit,
    RecordLimit,
    ScratchLimit,
    TruncatedHeader,
    HeaderVersion,
    RecordLength,
    TruncatedRecord,
    TrailingRecordBytes,
    MalformedSchema,
    MalformedBatch,
}
impl std::fmt::Display for CowSelectionCodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::UnsupportedType => "COW selection column type is outside the codec's closed set",
            Self::SchemaLimit => "COW selection schema exceeds its bounded profile",
            Self::SchemaChanged => "COW selection input schema differs from its first input",
            Self::BatchLimit => "COW selection exceeds its frozen batch count",
            Self::RecordLimit => "COW selection record exceeds its bounded profile",
            Self::ScratchLimit => "COW selection cursor exceeds its prepaid scratch capacity",
            Self::TruncatedHeader => "COW selection record header is truncated",
            Self::HeaderVersion => "unsupported COW selection record header",
            Self::RecordLength => "COW selection record length disagrees with its declarations",
            Self::TruncatedRecord => "COW selection record is truncated",
            Self::TrailingRecordBytes => "COW selection record has trailing bytes",
            Self::MalformedSchema => "malformed COW selection schema record",
            Self::MalformedBatch => "malformed COW selection batch record",
        })
    }
}
impl std::error::Error for CowSelectionCodecError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CowSelectionRecordKind {
    Schema,
    Batch,
}

/// One validated record declaration, read from its fixed prefix only.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CowSelectionRecordHeader {
    kind: CowSelectionRecordKind,
    record_bytes: u64,
    rows: u64,
    nodes: u32,
    buffers: u32,
}
impl CowSelectionRecordHeader {
    pub fn parse(prefix: &[u8]) -> Result<Self, CowSelectionCodecError> {
        if prefix.len() < COW_SELECTION_HEADER_BYTES {
            return Err(CowSelectionCodecError::TruncatedHeader);
        }
        if prefix[..4] != MAGIC || prefix[5..8] != [0; 3] {
            return Err(CowSelectionCodecError::HeaderVersion);
        }
        let u64_at = |at: usize| u64::from_le_bytes(prefix[at..at + 8].try_into().unwrap());
        let u32_at = |at: usize| u32::from_le_bytes(prefix[at..at + 4].try_into().unwrap());
        let header = Self {
            kind: match prefix[4] {
                KIND_SCHEMA => CowSelectionRecordKind::Schema,
                KIND_BATCH => CowSelectionRecordKind::Batch,
                _ => return Err(CowSelectionCodecError::HeaderVersion),
            },
            record_bytes: u64_at(8),
            rows: u64_at(16),
            nodes: u32_at(24),
            buffers: u32_at(28),
        };
        let nodes = header.nodes as usize;
        let shape_ok = match header.kind {
            CowSelectionRecordKind::Schema => header.rows == 0 && header.buffers == 0,
            CowSelectionRecordKind::Batch => {
                header.rows != 0 && (header.buffers as usize) <= 3 * nodes
            }
        };
        let minimum = (COW_SELECTION_HEADER_BYTES as u64)
            + 8 * (u64::from(header.buffers)
                + u64::from(header.nodes)
                    * u64::from(header.kind == CowSelectionRecordKind::Batch));
        if !shape_ok
            || nodes == 0
            || nodes > COW_SELECTION_MAX_NODES
            || header.record_bytes > COW_SELECTION_MAX_RECORD_BYTES
            || header.record_bytes < minimum
        {
            return Err(CowSelectionCodecError::RecordLength);
        }
        Ok(header)
    }
    pub const fn kind(self) -> CowSelectionRecordKind {
        self.kind
    }
    pub const fn record_bytes(self) -> u64 {
        self.record_bytes
    }
    pub const fn rows(self) -> u64 {
        self.rows
    }
    pub fn validate_record_bytes(self, available: usize) -> Result<(), CowSelectionCodecError> {
        match (available as u64).cmp(&self.record_bytes) {
            std::cmp::Ordering::Less => Err(CowSelectionCodecError::TruncatedRecord),
            std::cmp::Ordering::Greater => Err(CowSelectionCodecError::TrailingRecordBytes),
            std::cmp::Ordering::Equal => Ok(()),
        }
    }
    fn bytes(self) -> [u8; COW_SELECTION_HEADER_BYTES] {
        let mut bytes = [0; COW_SELECTION_HEADER_BYTES];
        bytes[..4].copy_from_slice(&MAGIC);
        bytes[4] = match self.kind {
            CowSelectionRecordKind::Schema => KIND_SCHEMA,
            CowSelectionRecordKind::Batch => KIND_BATCH,
        };
        bytes[8..16].copy_from_slice(&self.record_bytes.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.rows.to_le_bytes());
        bytes[24..28].copy_from_slice(&self.nodes.to_le_bytes());
        bytes[28..32].copy_from_slice(&self.buffers.to_le_bytes());
        bytes
    }
}

/// The first input's schema and the batch records completed so far.
/// Callers carry them across inputs; they are not selection evidence.
#[derive(Clone, Debug, Default)]
pub struct CowSelectionTotals {
    schema: Option<SchemaRef>,
    batches: usize,
    rows: u64,
}
impl CowSelectionTotals {
    pub const fn batches(&self) -> usize {
        self.batches
    }
    pub const fn rows(&self) -> u64 {
        self.rows
    }
    pub fn schema_sent(&self) -> bool {
        self.schema.is_some()
    }
}

/// Fixed-width carrier facts of one supported type, or its tag shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Shape {
    Null,
    Boolean,
    Fixed(usize),
    FixedBinary(usize),
    Bytes32,
    Bytes64,
    List32,
    List64,
    Struct,
    Dictionary,
}
fn shape(data_type: &DataType) -> Result<(u8, Shape), CowSelectionCodecError> {
    use DataType as D;
    use Shape as S;
    Ok(match data_type {
        D::Null => (tag::NULL, S::Null),
        D::Boolean => (tag::BOOLEAN, S::Boolean),
        D::Int8 => (tag::INT8, S::Fixed(1)),
        D::Int16 => (tag::INT16, S::Fixed(2)),
        D::Int32 => (tag::INT32, S::Fixed(4)),
        D::Int64 => (tag::INT64, S::Fixed(8)),
        D::UInt8 => (tag::UINT8, S::Fixed(1)),
        D::UInt16 => (tag::UINT16, S::Fixed(2)),
        D::UInt32 => (tag::UINT32, S::Fixed(4)),
        D::UInt64 => (tag::UINT64, S::Fixed(8)),
        D::Float32 => (tag::FLOAT32, S::Fixed(4)),
        D::Float64 => (tag::FLOAT64, S::Fixed(8)),
        D::Decimal128(_, _) => (tag::DECIMAL128, S::Fixed(16)),
        D::Decimal256(_, _) => (tag::DECIMAL256, S::Fixed(32)),
        D::Date32 => (tag::DATE32, S::Fixed(4)),
        D::Date64 => (tag::DATE64, S::Fixed(8)),
        D::Time32(TimeUnit::Second | TimeUnit::Millisecond) => (tag::TIME32, S::Fixed(4)),
        D::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond) => (tag::TIME64, S::Fixed(8)),
        D::Timestamp(_, _) => (tag::TIMESTAMP, S::Fixed(8)),
        D::FixedSizeBinary(width) if *width >= 0 => {
            (tag::FIXED_SIZE_BINARY, S::FixedBinary(*width as usize))
        }
        D::Utf8 => (tag::UTF8, S::Bytes32),
        D::LargeUtf8 => (tag::LARGE_UTF8, S::Bytes64),
        D::Binary => (tag::BINARY, S::Bytes32),
        D::LargeBinary => (tag::LARGE_BINARY, S::Bytes64),
        D::List(_) => (tag::LIST, S::List32),
        D::LargeList(_) => (tag::LARGE_LIST, S::List64),
        D::Struct(_) => (tag::STRUCT, S::Struct),
        D::Dictionary(key, value)
            if key.as_ref() == &D::Int32 && matches!(value.as_ref(), D::Utf8 | D::LargeUtf8) =>
        {
            (tag::DICTIONARY, S::Dictionary)
        }
        _ => return Err(CowSelectionCodecError::UnsupportedType),
    })
}
const fn buffer_count(shape: Shape) -> usize {
    match shape {
        Shape::Null | Shape::Struct => 1,
        Shape::Bytes32 | Shape::Bytes64 => 3,
        _ => 2,
    }
}

/// Counts and schema payload bytes of one field tree, without allocating.
#[derive(Default)]
struct Plan {
    nodes: usize,
    buffers: usize,
    schema_bytes: usize,
}
impl Plan {
    fn field(
        &mut self,
        name: &str,
        data_type: &DataType,
        depth: usize,
    ) -> Result<(), CowSelectionCodecError> {
        if depth > RootProfileV1::MAX_DEPTH {
            return Err(CowSelectionCodecError::SchemaLimit);
        }
        let (_, shape) = shape(data_type)?;
        self.nodes += 1;
        if self.nodes > COW_SELECTION_MAX_NODES {
            return Err(CowSelectionCodecError::SchemaLimit);
        }
        self.buffers += buffer_count(shape);
        self.schema_bytes = self
            .schema_bytes
            .checked_add(6 + name.len() + params_bytes(data_type))
            .ok_or(CowSelectionCodecError::SchemaLimit)?;
        match data_type {
            DataType::List(item) | DataType::LargeList(item) => {
                self.field(item.name(), item.data_type(), depth + 1)
            }
            DataType::Struct(fields) => fields
                .iter()
                .try_for_each(|child| self.field(child.name(), child.data_type(), depth + 1)),
            DataType::Dictionary(_, value) => self.field("", value, depth + 1),
            _ => Ok(()),
        }
    }
}
fn params_bytes(data_type: &DataType) -> usize {
    match data_type {
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => 2,
        DataType::Time32(_) | DataType::Time64(_) => 1,
        DataType::Timestamp(_, zone) => 6 + zone.as_deref().map_or(0, str::len),
        DataType::FixedSizeBinary(_) | DataType::Struct(_) => 4,
        _ => 0,
    }
}
fn unit_byte(unit: TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 1,
        TimeUnit::Microsecond => 2,
        TimeUnit::Nanosecond => 3,
    }
}
fn write_schema_node(out: &mut Vec<u8>, name: &str, nullable: bool, data_type: &DataType) {
    let (tag, _) = shape(data_type).expect("schema shape is planned first");
    out.push(tag);
    out.push(u8::from(nullable));
    out.extend_from_slice(&(name.len() as u32).to_le_bytes());
    out.extend_from_slice(name.as_bytes());
    match data_type {
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            out.push(*precision);
            out.push(*scale as u8);
        }
        DataType::Time32(unit) | DataType::Time64(unit) => out.push(unit_byte(*unit)),
        DataType::Timestamp(unit, zone) => {
            out.push(unit_byte(*unit));
            out.push(u8::from(zone.is_some()));
            let zone = zone.as_deref().unwrap_or("");
            out.extend_from_slice(&(zone.len() as u32).to_le_bytes());
            out.extend_from_slice(zone.as_bytes());
        }
        DataType::FixedSizeBinary(width) => out.extend_from_slice(&width.to_le_bytes()),
        DataType::Struct(fields) => out.extend_from_slice(&(fields.len() as u32).to_le_bytes()),
        _ => {}
    }
    match data_type {
        DataType::List(item) | DataType::LargeList(item) => {
            write_schema_node(out, item.name(), item.is_nullable(), item.data_type());
        }
        DataType::Struct(fields) => {
            for child in fields {
                write_schema_node(out, child.name(), child.is_nullable(), child.data_type());
            }
        }
        DataType::Dictionary(_, value) => write_schema_node(out, "", true, value),
        _ => {}
    }
}

/// One type node of one batch: its array and the logical row range it covers.
struct Node {
    array: ArrayRef,
    start: usize,
    len: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Source {
    Validity,
    BooleanValues,
    Fixed(usize),
    Offsets32,
    Offsets64,
    Data32,
    Data64,
    DictionaryKeys,
}
#[derive(Clone, Copy)]
struct Spec {
    node: usize,
    source: Source,
    bytes: u64,
}

fn primitive_bytes(array: &dyn Array) -> &[u8] {
    macro_rules! values {
        ($ty:ty) => {
            array.as_primitive::<$ty>().values().inner().as_slice()
        };
    }
    match array.data_type() {
        DataType::Int8 => values!(Int8Type),
        DataType::Int16 => values!(Int16Type),
        DataType::Int32 => values!(Int32Type),
        DataType::Int64 => values!(Int64Type),
        DataType::UInt8 => values!(UInt8Type),
        DataType::UInt16 => values!(UInt16Type),
        DataType::UInt32 => values!(UInt32Type),
        DataType::UInt64 => values!(UInt64Type),
        DataType::Float32 => values!(Float32Type),
        DataType::Float64 => values!(Float64Type),
        DataType::Decimal128(_, _) => values!(Decimal128Type),
        DataType::Decimal256(_, _) => values!(Decimal256Type),
        DataType::Date32 => values!(Date32Type),
        DataType::Date64 => values!(Date64Type),
        DataType::Time32(TimeUnit::Second) => values!(Time32SecondType),
        DataType::Time32(TimeUnit::Millisecond) => values!(Time32MillisecondType),
        DataType::Time64(TimeUnit::Microsecond) => values!(Time64MicrosecondType),
        DataType::Time64(TimeUnit::Nanosecond) => values!(Time64NanosecondType),
        DataType::Timestamp(TimeUnit::Second, _) => values!(TimestampSecondType),
        DataType::Timestamp(TimeUnit::Millisecond, _) => values!(TimestampMillisecondType),
        DataType::Timestamp(TimeUnit::Microsecond, _) => values!(TimestampMicrosecondType),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => values!(TimestampNanosecondType),
        DataType::FixedSizeBinary(_) => array.as_fixed_size_binary().value_data(),
        DataType::Dictionary(_, _) => array
            .as_dictionary::<Int32Type>()
            .keys()
            .values()
            .inner()
            .as_slice(),
        _ => unreachable!("fixed carriers are classified before emission"),
    }
}
fn offsets32(array: &dyn Array) -> &[i32] {
    match array.data_type() {
        DataType::Utf8 => array.as_string::<i32>().value_offsets(),
        DataType::Binary => array.as_binary::<i32>().value_offsets(),
        DataType::List(_) => array.as_list::<i32>().value_offsets(),
        _ => unreachable!("32-bit offsets are classified before emission"),
    }
}
fn offsets64(array: &dyn Array) -> &[i64] {
    match array.data_type() {
        DataType::LargeUtf8 => array.as_string::<i64>().value_offsets(),
        DataType::LargeBinary => array.as_binary::<i64>().value_offsets(),
        DataType::LargeList(_) => array.as_list::<i64>().value_offsets(),
        _ => unreachable!("64-bit offsets are classified before emission"),
    }
}
fn data_bytes(array: &dyn Array) -> &[u8] {
    match array.data_type() {
        DataType::Utf8 => array.as_string::<i32>().values().as_slice(),
        DataType::Binary => array.as_binary::<i32>().values().as_slice(),
        DataType::LargeUtf8 => array.as_string::<i64>().values().as_slice(),
        DataType::LargeBinary => array.as_binary::<i64>().values().as_slice(),
        _ => unreachable!("byte data is classified before emission"),
    }
}
fn null_buffer(array: &dyn Array) -> Option<&NullBuffer> {
    match array.data_type() {
        DataType::Null => None,
        _ => array.nulls(),
    }
}
/// One output byte of a re-packed bitmap that starts at `bit_start`.
fn bitmap_byte(source: &[u8], bit_start: usize, bits: usize, index: usize) -> u8 {
    let first = bit_start + 8 * index;
    let (byte, shift) = (first / 8, first % 8);
    let mut value = source[byte] >> shift;
    if shift != 0 && byte + 1 < source.len() {
        value |= source[byte + 1] << (8 - shift);
    }
    let remaining = bits - 8 * index;
    if remaining < 8 {
        value &= (1_u8 << remaining) - 1;
    }
    value
}

fn build_nodes(
    nodes: &mut Vec<Node>,
    specs: &mut Vec<Spec>,
    array: &ArrayRef,
    start: usize,
    len: usize,
) {
    let (_, shape) = shape(array.data_type()).expect("types are planned first");
    let index = nodes.len();
    nodes.push(Node {
        array: Arc::clone(array),
        start,
        len,
    });
    let validity = null_buffer(array.as_ref()).map_or(0, |_| len.div_ceil(8));
    specs.push(Spec {
        node: index,
        source: Source::Validity,
        bytes: validity as u64,
    });
    let mut push = |source, bytes: usize| {
        specs.push(Spec {
            node: index,
            source,
            bytes: bytes as u64,
        })
    };
    match shape {
        Shape::Null | Shape::Struct => {}
        Shape::Boolean => push(Source::BooleanValues, len.div_ceil(8)),
        Shape::Fixed(width) | Shape::FixedBinary(width) => push(Source::Fixed(width), len * width),
        Shape::Dictionary => push(Source::DictionaryKeys, len * 4),
        Shape::Bytes32 => {
            let offsets = offsets32(array.as_ref());
            push(Source::Offsets32, (len + 1) * 4);
            push(
                Source::Data32,
                (offsets[start + len] - offsets[start]) as usize,
            );
        }
        Shape::Bytes64 => {
            let offsets = offsets64(array.as_ref());
            push(Source::Offsets64, (len + 1) * 8);
            push(
                Source::Data64,
                (offsets[start + len] - offsets[start]) as usize,
            );
        }
        Shape::List32 => push(Source::Offsets32, (len + 1) * 4),
        Shape::List64 => push(Source::Offsets64, (len + 1) * 8),
    }
    match shape {
        Shape::List32 => {
            let offsets = offsets32(array.as_ref());
            let (child_start, child_end) = (offsets[start] as usize, offsets[start + len] as usize);
            let values = Arc::clone(array.as_list::<i32>().values());
            build_nodes(nodes, specs, &values, child_start, child_end - child_start);
        }
        Shape::List64 => {
            let offsets = offsets64(array.as_ref());
            let (child_start, child_end) = (offsets[start] as usize, offsets[start + len] as usize);
            let values = Arc::clone(array.as_list::<i64>().values());
            build_nodes(nodes, specs, &values, child_start, child_end - child_start);
        }
        Shape::Struct => {
            for child in array.as_struct().columns() {
                build_nodes(nodes, specs, child, start, len);
            }
        }
        Shape::Dictionary => {
            let values = Arc::clone(array.as_dictionary::<Int32Type>().values());
            let count = values.len();
            build_nodes(nodes, specs, &values, 0, count);
        }
        _ => {}
    }
}

#[derive(Clone, Copy)]
enum Phase {
    Prefix { offset: usize },
    Buffer { index: usize, offset: u64 },
    Complete,
}

pub struct CowSelectionEncoder {
    nodes: Vec<Node>,
    specs: Vec<Spec>,
    /// Optional SCHEMA record, then this batch's header and directory.
    prefix: Vec<u8>,
    rows: u64,
    phase: Phase,
    pending: CowSelectionTotals,
    totals: CowSelectionTotals,
}
impl CowSelectionEncoder {
    /// Exact heap capacity of the cursor for this input, plus its inline
    /// storage. The caller prepays it before construction.
    pub fn scratch_bytes(
        batch: &RecordBatch,
        totals: &CowSelectionTotals,
    ) -> Result<usize, CowSelectionCodecError> {
        let plan = Self::plan(batch, totals)?;
        Ok(Self::scratch_for(&plan, batch.num_rows(), totals))
    }
    fn plan(
        batch: &RecordBatch,
        totals: &CowSelectionTotals,
    ) -> Result<Plan, CowSelectionCodecError> {
        if let Some(schema) = &totals.schema
            && schema.fields() != batch.schema_ref().fields()
        {
            return Err(CowSelectionCodecError::SchemaChanged);
        }
        let mut plan = Plan::default();
        for field in batch.schema_ref().fields() {
            plan.field(field.name(), field.data_type(), 1)?;
        }
        if plan.nodes == 0 {
            return Err(CowSelectionCodecError::SchemaLimit);
        }
        Ok(plan)
    }
    /// The SCHEMA record (first input only) plus this batch's header and
    /// directory (non-empty input only).
    fn prefix_bytes(plan: &Plan, rows: usize, totals: &CowSelectionTotals) -> usize {
        let schema = if totals.schema.is_none() {
            COW_SELECTION_HEADER_BYTES + plan.schema_bytes
        } else {
            0
        };
        let directory = if rows == 0 {
            0
        } else {
            COW_SELECTION_HEADER_BYTES + 8 * (plan.nodes + plan.buffers)
        };
        schema + directory
    }
    fn scratch_for(plan: &Plan, rows: usize, totals: &CowSelectionTotals) -> usize {
        let tables = if rows == 0 {
            0
        } else {
            plan.nodes * size_of::<Node>() + plan.buffers * size_of::<Spec>()
        };
        size_of::<Self>() + tables + Self::prefix_bytes(plan, rows, totals)
    }
    pub fn try_new(
        batch: &RecordBatch,
        totals: CowSelectionTotals,
        prepaid_scratch_capacity: usize,
    ) -> Result<Self, CowSelectionCodecError> {
        let plan = Self::plan(batch, &totals)?;
        let rows = batch.num_rows();
        if rows != 0 && totals.batches >= MAX_CONNECTOR_ROW_MUTATION_SELECTION_BATCHES {
            return Err(CowSelectionCodecError::BatchLimit);
        }
        if Self::scratch_for(&plan, rows, &totals) > prepaid_scratch_capacity {
            return Err(CowSelectionCodecError::ScratchLimit);
        }
        let mut prefix = Vec::with_capacity(Self::prefix_bytes(&plan, rows, &totals));
        let mut pending = totals.clone();
        if totals.schema.is_none() {
            let record = (COW_SELECTION_HEADER_BYTES + plan.schema_bytes) as u64;
            if record > COW_SELECTION_MAX_RECORD_BYTES {
                return Err(CowSelectionCodecError::RecordLimit);
            }
            prefix.extend_from_slice(
                &CowSelectionRecordHeader {
                    kind: CowSelectionRecordKind::Schema,
                    record_bytes: record,
                    rows: 0,
                    nodes: plan.nodes as u32,
                    buffers: 0,
                }
                .bytes(),
            );
            for field in batch.schema_ref().fields() {
                write_schema_node(
                    &mut prefix,
                    field.name(),
                    field.is_nullable(),
                    field.data_type(),
                );
            }
            pending.schema = Some(batch.schema());
        }
        let (mut nodes, mut specs) = (Vec::new(), Vec::new());
        if rows != 0 {
            nodes.reserve_exact(plan.nodes);
            specs.reserve_exact(plan.buffers);
            for column in batch.columns() {
                build_nodes(&mut nodes, &mut specs, column, 0, rows);
            }
            debug_assert_eq!((nodes.len(), specs.len()), (plan.nodes, plan.buffers));
            let record = specs
                .iter()
                .try_fold(
                    (COW_SELECTION_HEADER_BYTES + 8 * (plan.nodes + plan.buffers)) as u64,
                    |total, spec| total.checked_add(spec.bytes),
                )
                .filter(|bytes| *bytes <= COW_SELECTION_MAX_RECORD_BYTES)
                .ok_or(CowSelectionCodecError::RecordLimit)?;
            prefix.extend_from_slice(
                &CowSelectionRecordHeader {
                    kind: CowSelectionRecordKind::Batch,
                    record_bytes: record,
                    rows: rows as u64,
                    nodes: plan.nodes as u32,
                    buffers: plan.buffers as u32,
                }
                .bytes(),
            );
            for node in &nodes {
                prefix.extend_from_slice(&(node.len as u64).to_le_bytes());
            }
            for spec in &specs {
                prefix.extend_from_slice(&spec.bytes.to_le_bytes());
            }
            pending.batches += 1;
            pending.rows += rows as u64;
        }
        Ok(Self {
            nodes,
            specs,
            phase: if prefix.is_empty() {
                Phase::Complete
            } else {
                Phase::Prefix { offset: 0 }
            },
            prefix,
            rows: rows as u64,
            totals: if rows == 0 && totals.schema.is_some() {
                pending.clone()
            } else {
                totals
            },
            pending,
        })
    }
    /// Totals of records whose entire encoding has been emitted.
    pub fn totals(&self) -> CowSelectionTotals {
        self.totals.clone()
    }
    /// Every limit was checked at construction, so emission cannot fail.
    pub fn step(&mut self, output: &mut [u8]) -> RenderTurn {
        let mut turn = RenderTurn {
            emitted_bytes: 0,
            examined_bytes: 0,
            visited_cells: 0,
            completed_rows: 0,
            status: RenderTurnStatus::Yielded,
        };
        loop {
            if matches!(self.phase, Phase::Complete) {
                turn.status = RenderTurnStatus::InputComplete;
                return turn;
            }
            if turn.visited_cells == COW_SELECTION_TURN_WORK
                || turn.examined_bytes == COW_SELECTION_TURN_BYTES
            {
                return turn;
            }
            turn.visited_cells += 1;
            match self.phase {
                Phase::Prefix { offset } => {
                    if offset == self.prefix.len() {
                        self.next_buffer(0, &mut turn);
                        continue;
                    }
                    if turn.emitted_bytes == output.len() {
                        turn.status = RenderTurnStatus::NeedsOutput;
                        return turn;
                    }
                    let count = (self.prefix.len() - offset)
                        .min(output.len() - turn.emitted_bytes)
                        .min(COW_SELECTION_TURN_BYTES - turn.examined_bytes);
                    output[turn.emitted_bytes..turn.emitted_bytes + count]
                        .copy_from_slice(&self.prefix[offset..offset + count]);
                    turn.emitted_bytes += count;
                    turn.examined_bytes += count;
                    self.phase = Phase::Prefix {
                        offset: offset + count,
                    };
                }
                Phase::Buffer { index, offset } => {
                    let spec = self.specs[index];
                    if offset == spec.bytes {
                        self.next_buffer(index + 1, &mut turn);
                        continue;
                    }
                    if turn.emitted_bytes == output.len() {
                        turn.status = RenderTurnStatus::NeedsOutput;
                        return turn;
                    }
                    let limit = ((spec.bytes - offset) as usize)
                        .min(output.len() - turn.emitted_bytes)
                        .min(COW_SELECTION_TURN_BYTES - turn.examined_bytes);
                    let target = &mut output[turn.emitted_bytes..turn.emitted_bytes + limit];
                    fill(&self.nodes[spec.node], spec.source, offset as usize, target);
                    turn.emitted_bytes += limit;
                    turn.examined_bytes += limit;
                    self.phase = Phase::Buffer {
                        index,
                        offset: offset + limit as u64,
                    };
                }
                Phase::Complete => unreachable!("the terminal phase is checked first"),
            }
        }
    }
    fn next_buffer(&mut self, index: usize, turn: &mut RenderTurn) {
        if index < self.specs.len() {
            self.phase = Phase::Buffer { index, offset: 0 };
        } else {
            self.phase = Phase::Complete;
            self.totals = self.pending.clone();
            turn.completed_rows += self.rows;
        }
    }
}

/// Copy `target.len()` bytes of one logical buffer, starting at `offset`.
fn fill(node: &Node, source: Source, offset: usize, target: &mut [u8]) {
    let array = node.array.as_ref();
    match source {
        Source::Validity => {
            let nulls = null_buffer(array).expect("a validity buffer has a null source");
            let bit_start = nulls.offset() + node.start;
            for (at, byte) in target.iter_mut().enumerate() {
                *byte = bitmap_byte(nulls.buffer().as_slice(), bit_start, node.len, offset + at);
            }
        }
        Source::BooleanValues => {
            let values = array.as_boolean().values();
            let bit_start = values.offset() + node.start;
            for (at, byte) in target.iter_mut().enumerate() {
                *byte = bitmap_byte(values.values(), bit_start, node.len, offset + at);
            }
        }
        Source::Fixed(width) => {
            let base = node.start * width + offset;
            target.copy_from_slice(&primitive_bytes(array)[base..base + target.len()]);
        }
        Source::DictionaryKeys => {
            let base = node.start * 4 + offset;
            target.copy_from_slice(&primitive_bytes(array)[base..base + target.len()]);
        }
        Source::Offsets32 => {
            let offsets = offsets32(array);
            let base = offsets[node.start];
            for (at, byte) in target.iter_mut().enumerate() {
                let position = offset + at;
                let value = offsets[node.start + position / 4] - base;
                *byte = value.to_le_bytes()[position % 4];
            }
        }
        Source::Offsets64 => {
            let offsets = offsets64(array);
            let base = offsets[node.start];
            for (at, byte) in target.iter_mut().enumerate() {
                let position = offset + at;
                let value = offsets[node.start + position / 8] - base;
                *byte = value.to_le_bytes()[position % 8];
            }
        }
        Source::Data32 => {
            let base = offsets32(array)[node.start] as usize + offset;
            target.copy_from_slice(&data_bytes(array)[base..base + target.len()]);
        }
        Source::Data64 => {
            let base = offsets64(array)[node.start] as usize + offset;
            target.copy_from_slice(&data_bytes(array)[base..base + target.len()]);
        }
    }
}

/// Frontend-side decoding of complete, length-validated records.
pub struct CowSelectionDecoder;
impl CowSelectionDecoder {
    pub fn decode_schema(record: &[u8]) -> Result<SchemaRef, CowSelectionCodecError> {
        let header = CowSelectionRecordHeader::parse(record)?;
        header.validate_record_bytes(record.len())?;
        if header.kind != CowSelectionRecordKind::Schema {
            return Err(CowSelectionCodecError::MalformedSchema);
        }
        let mut reader = Reader::new(&record[COW_SELECTION_HEADER_BYTES..]);
        let mut fields = Vec::new();
        let mut nodes = 0;
        while !reader.done() {
            fields.push(reader.field(&mut nodes, 1)?);
        }
        if nodes != header.nodes as usize {
            return Err(CowSelectionCodecError::MalformedSchema);
        }
        Ok(Arc::new(Schema::new(fields)))
    }
    pub fn decode_batch(
        schema: &SchemaRef,
        record: &[u8],
    ) -> Result<RecordBatch, CowSelectionCodecError> {
        let header = CowSelectionRecordHeader::parse(record)?;
        header.validate_record_bytes(record.len())?;
        if header.kind != CowSelectionRecordKind::Batch {
            return Err(CowSelectionCodecError::MalformedBatch);
        }
        let malformed = CowSelectionCodecError::MalformedBatch;
        let nodes = header.nodes as usize;
        let buffers = header.buffers as usize;
        let directory = &record[COW_SELECTION_HEADER_BYTES..];
        let word = |index: usize| {
            u64::from_le_bytes(directory[8 * index..8 * index + 8].try_into().unwrap())
        };
        let mut state = BatchState {
            lengths: (0..nodes).map(word).collect(),
            sizes: (nodes..nodes + buffers).map(word).collect(),
            payload: &record[COW_SELECTION_HEADER_BYTES + 8 * (nodes + buffers)..],
            node: 0,
            buffer: 0,
            position: 0,
        };
        let rows = usize::try_from(header.rows).map_err(|_| malformed)?;
        let mut columns = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            let data = state.array(field.data_type())?;
            if data.len() != rows {
                return Err(malformed);
            }
            columns.push(make_array(data));
        }
        if state.node != nodes || state.buffer != buffers || state.position != state.payload.len() {
            return Err(malformed);
        }
        RecordBatch::try_new_with_options(
            Arc::clone(schema),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .map_err(|_| malformed)
    }
}

/// A whole COW selection stream: exactly one SCHEMA record first, then
/// BATCH records within the frozen batch count. Each decoded batch is handed
/// to the caller at once; the decoder retains only the schema and a count.
#[derive(Debug, Default)]
pub struct CowSelectionStreamDecoder {
    schema: Option<SchemaRef>,
    batches: usize,
}

impl CowSelectionStreamDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// The schema of the SCHEMA record, once it has been applied.
    pub fn schema(&self) -> Option<&SchemaRef> {
        self.schema.as_ref()
    }

    /// Apply one complete, assembled record. A BATCH record yields its
    /// decoded batch; the SCHEMA record yields nothing.
    pub fn apply_record(
        &mut self,
        record: &[u8],
    ) -> Result<Option<RecordBatch>, CowSelectionCodecError> {
        let header = CowSelectionRecordHeader::parse(record)?;
        match (header.kind, &self.schema) {
            (CowSelectionRecordKind::Schema, None) => {
                self.schema = Some(CowSelectionDecoder::decode_schema(record)?);
                Ok(None)
            }
            (CowSelectionRecordKind::Schema, Some(_)) => {
                Err(CowSelectionCodecError::MalformedSchema)
            }
            (CowSelectionRecordKind::Batch, None) => Err(CowSelectionCodecError::MalformedBatch),
            (CowSelectionRecordKind::Batch, Some(schema)) => {
                if self.batches >= MAX_CONNECTOR_ROW_MUTATION_SELECTION_BATCHES {
                    return Err(CowSelectionCodecError::BatchLimit);
                }
                let batch = CowSelectionDecoder::decode_batch(schema, record)?;
                self.batches += 1;
                Ok(Some(batch))
            }
        }
    }

    /// The stream's End. A selection with no input at all has no schema;
    /// the consumer applies its own frozen layout.
    pub fn finish(self) -> Option<SchemaRef> {
        self.schema
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}
impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }
    fn done(&self) -> bool {
        self.position == self.bytes.len()
    }
    fn take(&mut self, count: usize) -> Result<&'a [u8], CowSelectionCodecError> {
        let end = self
            .position
            .checked_add(count)
            .filter(|end| *end <= self.bytes.len())
            .ok_or(CowSelectionCodecError::MalformedSchema)?;
        let slice = &self.bytes[self.position..end];
        self.position = end;
        Ok(slice)
    }
    fn u8(&mut self) -> Result<u8, CowSelectionCodecError> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, CowSelectionCodecError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn text(&mut self) -> Result<String, CowSelectionCodecError> {
        let len = self.u32()? as usize;
        std::str::from_utf8(self.take(len)?)
            .map(str::to_owned)
            .map_err(|_| CowSelectionCodecError::MalformedSchema)
    }
    fn unit(&mut self) -> Result<TimeUnit, CowSelectionCodecError> {
        Ok(match self.u8()? {
            0 => TimeUnit::Second,
            1 => TimeUnit::Millisecond,
            2 => TimeUnit::Microsecond,
            3 => TimeUnit::Nanosecond,
            _ => return Err(CowSelectionCodecError::MalformedSchema),
        })
    }
    fn field(&mut self, nodes: &mut usize, depth: usize) -> Result<Field, CowSelectionCodecError> {
        let malformed = CowSelectionCodecError::MalformedSchema;
        *nodes += 1;
        if *nodes > COW_SELECTION_MAX_NODES || depth > RootProfileV1::MAX_DEPTH {
            return Err(malformed);
        }
        let tag = self.u8()?;
        let nullable = match self.u8()? {
            0 => false,
            1 => true,
            _ => return Err(malformed),
        };
        let name = self.text()?;
        let data_type = match tag {
            tag::NULL => DataType::Null,
            tag::BOOLEAN => DataType::Boolean,
            tag::INT8 => DataType::Int8,
            tag::INT16 => DataType::Int16,
            tag::INT32 => DataType::Int32,
            tag::INT64 => DataType::Int64,
            tag::UINT8 => DataType::UInt8,
            tag::UINT16 => DataType::UInt16,
            tag::UINT32 => DataType::UInt32,
            tag::UINT64 => DataType::UInt64,
            tag::FLOAT32 => DataType::Float32,
            tag::FLOAT64 => DataType::Float64,
            tag::DECIMAL128 | tag::DECIMAL256 => {
                let precision = self.u8()?;
                let scale = self.u8()? as i8;
                if tag == tag::DECIMAL128 {
                    DataType::Decimal128(precision, scale)
                } else {
                    DataType::Decimal256(precision, scale)
                }
            }
            tag::DATE32 => DataType::Date32,
            tag::DATE64 => DataType::Date64,
            tag::TIME32 => DataType::Time32(self.unit()?),
            tag::TIME64 => DataType::Time64(self.unit()?),
            tag::TIMESTAMP => {
                let unit = self.unit()?;
                let has_zone = self.u8()?;
                let zone = self.text()?;
                match has_zone {
                    0 if zone.is_empty() => DataType::Timestamp(unit, None),
                    1 => DataType::Timestamp(unit, Some(zone.into())),
                    _ => return Err(malformed),
                }
            }
            tag::FIXED_SIZE_BINARY => {
                DataType::FixedSizeBinary(i32::from_le_bytes(self.take(4)?.try_into().unwrap()))
            }
            tag::UTF8 => DataType::Utf8,
            tag::LARGE_UTF8 => DataType::LargeUtf8,
            tag::BINARY => DataType::Binary,
            tag::LARGE_BINARY => DataType::LargeBinary,
            tag::LIST => DataType::List(Arc::new(self.field(nodes, depth + 1)?)),
            tag::LARGE_LIST => DataType::LargeList(Arc::new(self.field(nodes, depth + 1)?)),
            tag::STRUCT => {
                let count = self.u32()? as usize;
                if count > COW_SELECTION_MAX_NODES {
                    return Err(malformed);
                }
                let mut children = Vec::with_capacity(count);
                for _ in 0..count {
                    children.push(self.field(nodes, depth + 1)?);
                }
                DataType::Struct(Fields::from(children))
            }
            tag::DICTIONARY => {
                let value = self.field(nodes, depth + 1)?;
                DataType::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(value.data_type().clone()),
                )
            }
            _ => return Err(malformed),
        };
        // The decoded schema must itself be inside the closed set.
        shape(&data_type).map_err(|_| malformed)?;
        Ok(Field::new(name, data_type, nullable))
    }
}

struct BatchState<'a> {
    lengths: Vec<u64>,
    sizes: Vec<u64>,
    payload: &'a [u8],
    node: usize,
    buffer: usize,
    position: usize,
}
impl BatchState<'_> {
    fn buffer(
        &mut self,
        expected: Option<usize>,
    ) -> Result<Option<Buffer>, CowSelectionCodecError> {
        let malformed = CowSelectionCodecError::MalformedBatch;
        let size = usize::try_from(*self.sizes.get(self.buffer).ok_or(malformed)?)
            .map_err(|_| malformed)?;
        self.buffer += 1;
        if expected.is_some_and(|expected| expected != size) {
            return Err(malformed);
        }
        let end = self
            .position
            .checked_add(size)
            .filter(|end| *end <= self.payload.len())
            .ok_or(malformed)?;
        let bytes = &self.payload[self.position..end];
        self.position = end;
        Ok((size != 0 || expected.is_some()).then(|| Buffer::from_slice_ref(bytes)))
    }
    fn array(&mut self, data_type: &DataType) -> Result<ArrayData, CowSelectionCodecError> {
        let malformed = CowSelectionCodecError::MalformedBatch;
        let (_, shape) = shape(data_type).map_err(|_| malformed)?;
        let len = usize::try_from(*self.lengths.get(self.node).ok_or(malformed)?)
            .map_err(|_| malformed)?;
        self.node += 1;
        let validity = self.buffer(None)?;
        // Non-nullable columns may still carry an all-valid bitmap; the
        // assembled RecordBatch rejects any actual null in them.
        if validity
            .as_ref()
            .is_some_and(|bytes| bytes.len() != len.div_ceil(8) || matches!(shape, Shape::Null))
        {
            return Err(malformed);
        }
        let mut builder = ArrayData::builder(data_type.clone())
            .len(len)
            .null_bit_buffer(validity);
        match shape {
            Shape::Null | Shape::Struct => {}
            Shape::Boolean => {
                builder = builder.add_buffer(self.buffer(Some(len.div_ceil(8)))?.unwrap())
            }
            Shape::Fixed(width) | Shape::FixedBinary(width) => {
                let bytes = len.checked_mul(width).ok_or(malformed)?;
                builder = builder.add_buffer(self.buffer(Some(bytes))?.unwrap());
            }
            Shape::Dictionary => {
                let bytes = len.checked_mul(4).ok_or(malformed)?;
                builder = builder.add_buffer(self.buffer(Some(bytes))?.unwrap());
            }
            Shape::Bytes32 | Shape::List32 | Shape::Bytes64 | Shape::List64 => {
                let width = if matches!(shape, Shape::Bytes32 | Shape::List32) {
                    4
                } else {
                    8
                };
                let bytes = len
                    .checked_add(1)
                    .and_then(|n| n.checked_mul(width))
                    .ok_or(malformed)?;
                builder = builder.add_buffer(self.buffer(Some(bytes))?.unwrap());
                if matches!(shape, Shape::Bytes32 | Shape::Bytes64) {
                    let data = self
                        .buffer(None)?
                        .unwrap_or_else(|| Buffer::from_slice_ref([0_u8; 0]));
                    builder = builder.add_buffer(data);
                }
            }
        }
        match data_type {
            DataType::List(item) | DataType::LargeList(item) => {
                builder = builder.add_child_data(self.array(item.data_type())?);
            }
            DataType::Struct(fields) => {
                for child in fields {
                    let data = self.array(child.data_type())?;
                    if data.len() != len {
                        return Err(malformed);
                    }
                    builder = builder.add_child_data(data);
                }
            }
            DataType::Dictionary(_, value) => {
                builder = builder.add_child_data(self.array(value)?);
            }
            _ => {}
        }
        builder.build().map_err(|_| malformed)
    }
}
