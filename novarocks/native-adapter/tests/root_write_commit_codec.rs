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

//! Module-only PreparedWriteCommitV1 codec evidence: exact records, limits
//! refused before a byte is written, bounded turns and no cursor allocation.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, Int8Array, Int32Array, Int64Array, ListArray, MapArray, RecordBatch,
    StringArray, StructArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Schema};
use novarocks_native_adapter::root_write_commit_codec::*;
use novarocks_result_render::RenderTurnStatus;
use novarocks_spi::connector::MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES;
use novarocks_spi::connector::write_stack::{
    MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES, MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES, RootRowKind,
    root_write_result_schema,
};

thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}
struct Probe;
fn allocated() {
    if TRACK.try_with(Cell::get).unwrap_or(false) {
        let _ = ALLOCATIONS.try_with(|calls| calls.set(calls.get() + 1));
    }
}
// SAFETY: All requests are forwarded unchanged to System; observation uses
// allocation-free per-thread scalar cells and never allocation contents.
unsafe impl GlobalAlloc for Probe {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        allocated();
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        allocated();
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        allocated();
        unsafe { System.realloc(ptr, layout, size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: Probe = Probe;

#[derive(Clone)]
enum Row {
    Summary(i64),
    Fragment(i32, Vec<u8>),
    Artifact {
        target: i32,
        fields: Vec<i32>,
        blob: String,
        body: Vec<u8>,
        properties: Vec<(String, String)>,
    },
    /// Raw kind and presence, for shape refusals.
    Raw {
        kind: i8,
        target: Option<i32>,
        count: Option<i64>,
        fragment: Option<Vec<u8>>,
    },
}

fn artifact(
    target: i32,
    fields: &[i32],
    blob: &str,
    body: &[u8],
    properties: &[(&str, &str)],
) -> Row {
    Row::Artifact {
        target,
        fields: fields.to_vec(),
        blob: blob.into(),
        body: body.to_vec(),
        properties: properties
            .iter()
            .map(|(key, value)| ((*key).into(), (*value).into()))
            .collect(),
    }
}

fn batch(rows: &[Row]) -> RecordBatch {
    let schema = root_write_result_schema();
    let mut kinds = Vec::new();
    let mut targets = Vec::new();
    let mut counts = Vec::new();
    let mut fragments: Vec<Option<Vec<u8>>> = Vec::new();
    let mut field_lengths = Vec::new();
    let mut field_values = Vec::new();
    let mut field_valid = Vec::new();
    let mut blobs: Vec<Option<String>> = Vec::new();
    let mut bodies: Vec<Option<Vec<u8>>> = Vec::new();
    let mut entry_lengths = Vec::new();
    let mut keys = Vec::new();
    let mut values = Vec::new();
    let mut entry_valid = Vec::new();
    for row in rows {
        let (kind, target, count, fragment, artifact) = match row.clone() {
            Row::Summary(count) => (1, None, Some(count), None, None),
            Row::Fragment(target, bytes) => (2, Some(target), None, Some(bytes), None),
            Row::Artifact {
                target,
                fields,
                blob,
                body,
                properties,
            } => (
                3,
                Some(target),
                None,
                None,
                Some((fields, blob, body, properties)),
            ),
            Row::Raw {
                kind,
                target,
                count,
                fragment,
            } => (kind, target, count, fragment, None),
        };
        kinds.push(kind);
        targets.push(target);
        counts.push(count);
        fragments.push(fragment);
        match artifact {
            Some((fields, blob, body, properties)) => {
                field_lengths.push(fields.len());
                field_values.extend(fields);
                field_valid.push(true);
                blobs.push(Some(blob));
                bodies.push(Some(body));
                entry_lengths.push(properties.len());
                for (key, value) in properties {
                    keys.push(key);
                    values.push(value);
                }
                entry_valid.push(true);
            }
            None => {
                field_lengths.push(0);
                field_valid.push(false);
                blobs.push(None);
                bodies.push(None);
                entry_lengths.push(0);
                entry_valid.push(false);
            }
        }
    }
    let DataType::List(item) = schema.field(4).data_type() else {
        unreachable!()
    };
    let DataType::Map(entries, false) = schema.field(7).data_type() else {
        unreachable!()
    };
    let DataType::Struct(entry_fields) = entries.data_type() else {
        unreachable!()
    };
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int8Array::from(kinds)),
        Arc::new(Int32Array::from(targets)),
        Arc::new(Int64Array::from(counts)),
        Arc::new(BinaryArray::from_iter(fragments)),
        Arc::new(ListArray::new(
            item.clone(),
            OffsetBuffer::from_lengths(field_lengths),
            Arc::new(Int32Array::from(field_values)),
            Some(field_valid.into()),
        )),
        Arc::new(StringArray::from(blobs)),
        Arc::new(BinaryArray::from_iter(bodies)),
        Arc::new(MapArray::new(
            entries.clone(),
            OffsetBuffer::from_lengths(entry_lengths),
            StructArray::new(
                entry_fields.clone(),
                vec![
                    Arc::new(StringArray::from(keys)) as ArrayRef,
                    Arc::new(StringArray::from(values)) as ArrayRef,
                ],
                None,
            ),
            Some(entry_valid.into()),
            false,
        )),
    ];
    RecordBatch::try_new(schema, columns).unwrap()
}

struct Encoded {
    bytes: Vec<u8>,
    totals: WriteCommitTotals,
    completed: u64,
    error: Option<WriteCommitCodecError>,
}
fn encode(batch: RecordBatch, totals: WriteCommitTotals, output: usize) -> Encoded {
    let mut encoder = WriteCommitEncoder::try_new(batch, totals).unwrap();
    let mut buffer = vec![0_u8; output];
    let mut encoded = Encoded {
        bytes: Vec::new(),
        totals,
        completed: 0,
        error: None,
    };
    loop {
        TRACK.with(|track| track.set(true));
        ALLOCATIONS.with(|calls| calls.set(0));
        let turn = encoder.step(&mut buffer);
        TRACK.with(|track| track.set(false));
        assert_eq!(ALLOCATIONS.with(Cell::get), 0, "cursor step allocated");
        let turn = match turn {
            Ok(turn) => turn,
            Err(error) => {
                encoded.error = Some(error);
                encoded.totals = encoder.totals();
                return encoded;
            }
        };
        assert!(turn.examined_bytes <= WRITE_COMMIT_TURN_BYTES);
        assert!(turn.visited_cells <= WRITE_COMMIT_TURN_WORK);
        encoded
            .bytes
            .extend_from_slice(&buffer[..turn.emitted_bytes]);
        encoded.completed += turn.completed_rows;
        if turn.status == RenderTurnStatus::InputComplete {
            encoded.totals = encoder.totals();
            return encoded;
        }
    }
}
fn records(mut bytes: &[u8]) -> Vec<(WriteCommitRecordHeader, Vec<u8>)> {
    let mut out = Vec::new();
    while !bytes.is_empty() {
        let header = WriteCommitRecordHeader::parse(bytes).unwrap();
        let record = &bytes[..header.record_bytes()];
        header.validate_record_bytes(record.len()).unwrap();
        out.push((header, record[WRITE_COMMIT_HEADER_BYTES..].to_vec()));
        bytes = &bytes[header.record_bytes()..];
    }
    out
}

#[test]
fn summary_fragment_and_artifact_records_are_exact() {
    let rows = [
        Row::Fragment(2, b"frag".to_vec()),
        artifact(2, &[7, 3], "theta", &[0, 255], &[("k", "vv"), ("a", "")]),
        Row::Summary(41),
    ];
    let encoded = encode(batch(&rows), WriteCommitTotals::default(), 1 << 20);
    assert!(encoded.error.is_none());
    assert_eq!(encoded.completed, 3);
    let records = records(&encoded.bytes);
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].0.kind(), RootRowKind::PreparedFragment);
    assert_eq!(records[0].0.target(), 2);
    assert_eq!(records[0].1, b"frag");
    let (header, payload) = &records[1];
    assert_eq!(header.kind(), RootRowKind::ArtifactDraft);
    assert_eq!(
        (
            header.field_count(),
            header.blob_type_bytes(),
            header.body_bytes()
        ),
        (2, 5, 2)
    );
    assert_eq!((header.property_count(), header.property_bytes()), (2, 4));
    let mut expected = Vec::new();
    expected.extend_from_slice(&7_i32.to_le_bytes());
    expected.extend_from_slice(&3_i32.to_le_bytes());
    expected.extend_from_slice(b"theta");
    expected.extend_from_slice(&[0, 255]);
    for (key, value) in [("k", "vv"), ("a", "")] {
        expected.extend_from_slice(&(key.len() as u32).to_le_bytes());
        expected.extend_from_slice(key.as_bytes());
        expected.extend_from_slice(&(value.len() as u32).to_le_bytes());
        expected.extend_from_slice(value.as_bytes());
    }
    assert_eq!(payload, &expected);
    assert_eq!(records[2].0.kind(), RootRowKind::Summary);
    assert_eq!(records[2].0.row_count(), 41);
    assert!(records[2].1.is_empty());
    assert!(encoded.totals.summary_seen());
    assert_eq!(encoded.totals.fragments(), 1);
    assert_eq!(encoded.totals.fragment_bytes(), 4);
    assert_eq!(encoded.totals.artifacts(), 1);
    encoded.totals.finish().unwrap();
}

#[test]
fn tiny_outputs_and_large_bodies_produce_identical_bytes_in_bounded_turns() {
    let body = vec![0x5a; 200 * 1024];
    let rows = [
        artifact(1, &[1], "b", &body, &[]),
        Row::Fragment(1, vec![9; 70 * 1024]),
        Row::Summary(0),
    ];
    let whole = encode(batch(&rows), WriteCommitTotals::default(), 1 << 20);
    let tiny = encode(batch(&rows), WriteCommitTotals::default(), 7);
    assert!(whole.error.is_none() && tiny.error.is_none());
    assert_eq!(whole.bytes, tiny.bytes);
    assert_eq!(records(&whole.bytes)[0].1.len(), 4 + 1 + body.len());
}

#[test]
fn a_second_summary_is_refused_before_its_header_and_across_batches() {
    let encoded = encode(
        batch(&[Row::Summary(1), Row::Summary(2)]),
        WriteCommitTotals::default(),
        1 << 20,
    );
    // The failing turn's output is discarded by its owner; only the first
    // SUMMARY completed, and the second was refused at its preflight.
    assert_eq!(encoded.error, Some(WriteCommitCodecError::DuplicateSummary));
    assert!(encoded.totals.summary_seen());
    let first = encode(
        batch(&[Row::Summary(1)]),
        WriteCommitTotals::default(),
        1 << 20,
    );
    let second = encode(batch(&[Row::Summary(1)]), first.totals, 1 << 20);
    assert_eq!(second.error, Some(WriteCommitCodecError::DuplicateSummary));
    assert!(second.bytes.is_empty());
    assert_eq!(
        WriteCommitTotals::default().finish(),
        Err(WriteCommitCodecError::MissingSummary)
    );
}

#[test]
fn fragment_single_value_and_entry_budgets_are_refused_before_emission() {
    let oversized = encode(
        batch(&[Row::Fragment(
            1,
            vec![0; MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES + 1],
        )]),
        WriteCommitTotals::default(),
        1 << 20,
    );
    assert_eq!(oversized.error, Some(WriteCommitCodecError::FragmentLimit));
    assert!(oversized.bytes.is_empty());
    let exact = encode(
        batch(&[Row::Fragment(
            1,
            vec![0; MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES],
        )]),
        WriteCommitTotals::default(),
        1 << 20,
    );
    assert!(exact.error.is_none());
    let many = vec![Row::Fragment(1, vec![1]); MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES + 1];
    let entries = encode(batch(&many), WriteCommitTotals::default(), 1 << 20);
    assert_eq!(entries.error, Some(WriteCommitCodecError::FragmentLimit));
    assert_eq!(
        entries.totals.fragments(),
        MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES
    );
}

#[test]
fn shapes_targets_and_artifact_values_are_refused_before_emission() {
    let long_value = "v".repeat(MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES);
    let cases = [
        (
            Row::Raw {
                kind: 1,
                target: Some(1),
                count: Some(1),
                fragment: None,
            },
            WriteCommitCodecError::Shape,
        ),
        (
            Row::Raw {
                kind: 9,
                target: None,
                count: Some(1),
                fragment: None,
            },
            WriteCommitCodecError::Kind,
        ),
        (Row::Summary(-1), WriteCommitCodecError::RowCount),
        (Row::Fragment(-3, vec![1]), WriteCommitCodecError::Target),
        (
            artifact(1, &[], "b", &[], &[]),
            WriteCommitCodecError::FieldIds,
        ),
        (
            artifact(1, &[0], "b", &[], &[]),
            WriteCommitCodecError::FieldIds,
        ),
        (
            artifact(1, &[1], "", &[], &[]),
            WriteCommitCodecError::BlobType,
        ),
        (
            artifact(1, &[1], "b", &[], &[("", "v")]),
            WriteCommitCodecError::PropertyKey,
        ),
        (
            artifact(1, &[1], "b", &[], &[("k", long_value.as_str())]),
            WriteCommitCodecError::PropertyLimit,
        ),
    ];
    for (row, error) in cases {
        let encoded = encode(batch(&[row]), WriteCommitTotals::default(), 1 << 20);
        assert_eq!(encoded.error, Some(error));
        assert!(encoded.bytes.is_empty(), "{error:?} emitted bytes");
    }
}

#[test]
fn foreign_schema_is_refused_at_construction() {
    let schema = Arc::new(Schema::new(vec![Field::new("kind", DataType::Int8, false)]));
    let foreign =
        RecordBatch::try_new(schema, vec![Arc::new(Int8Array::from(vec![1])) as ArrayRef]).unwrap();
    assert!(matches!(
        WriteCommitEncoder::try_new(foreign, WriteCommitTotals::default()),
        Err(WriteCommitCodecError::Schema)
    ));
}

#[test]
fn header_parse_refuses_forged_declarations() {
    let encoded = encode(
        batch(&[Row::Fragment(1, b"x".to_vec())]),
        WriteCommitTotals::default(),
        1 << 20,
    );
    let header = encoded.bytes[..WRITE_COMMIT_HEADER_BYTES].to_vec();
    assert!(WriteCommitRecordHeader::parse(&header).is_ok());
    let forged = |at: usize, value: u8| {
        let mut bytes = header.clone();
        bytes[at] = value;
        WriteCommitRecordHeader::parse(&bytes)
    };
    assert_eq!(forged(0, b'Q'), Err(WriteCommitCodecError::HeaderVersion));
    assert_eq!(forged(10, 1), Err(WriteCommitCodecError::HeaderVersion));
    assert_eq!(forged(8, 7), Err(WriteCommitCodecError::Kind));
    assert_eq!(forged(4, 0), Err(WriteCommitCodecError::RecordLength));
    // A fragment record that also declares an artifact field.
    assert_eq!(forged(28, 1), Err(WriteCommitCodecError::Shape));
    assert_eq!(
        WriteCommitRecordHeader::parse(&header[..WRITE_COMMIT_HEADER_BYTES - 1]),
        Err(WriteCommitCodecError::TruncatedHeader)
    );
    let parsed = WriteCommitRecordHeader::parse(&header).unwrap();
    assert_eq!(
        parsed.validate_record_bytes(WRITE_COMMIT_HEADER_BYTES),
        Err(WriteCommitCodecError::TruncatedRecord)
    );
    assert_eq!(
        parsed.validate_record_bytes(WRITE_COMMIT_HEADER_BYTES + 2),
        Err(WriteCommitCodecError::TrailingRecordBytes)
    );
}
