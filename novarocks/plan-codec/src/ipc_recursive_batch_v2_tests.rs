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

use super::*;
use arrow::{
    array::*,
    datatypes::Schema,
    ipc::{
        self,
        writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions},
    },
    record_batch::RecordBatch,
};
use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use flatbuffers::FlatBufferBuilder;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    calls: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut calls = self.calls.lock().unwrap();
        let at = calls.len();
        if let Some((stop, _)) = self.stop {
            assert!(at <= stop, "callback after primary refusal");
        }
        calls.push((phase, units));
        match self.stop {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn limits() -> RecursiveBatchProjectionLimits {
    RecursiveBatchProjectionLimits {
        flat: FlatBatchProjectionLimits {
            max_metadata_bytes: 1 << 20,
            max_body_bytes: 1 << 20,
            max_rows: 4096,
            max_buffer_descriptors: 4096,
            max_view_validation_bytes: 1 << 20,
        },
        max_field_nodes: 4096,
        max_total_rows: 1 << 20,
        max_geometry_request_bytes: 1 << 20,
    }
}
fn verifier() -> VerifierOptions {
    VerifierOptions {
        max_depth: 67,
        max_tables: 4096,
        max_apparent_size: 1 << 22,
        ignore_missing_null_terminator: false,
    }
}
#[allow(deprecated)]
fn encoded(array: ArrayRef) -> (Field, Vec<u8>, Vec<u8>) {
    let field = Field::new("actual", array.data_type().clone(), true);
    let batch =
        RecordBatch::try_new(Arc::new(Schema::new(vec![field.clone()])), vec![array]).unwrap();
    let (dict, batch) = IpcDataGenerator::default()
        .encoded_batch(
            &batch,
            &mut DictionaryTracker::new(false),
            &IpcWriteOptions::default(),
        )
        .unwrap();
    assert!(dict.is_empty());
    (field, batch.ipc_message, batch.arrow_data)
}
fn checked<'a>(
    field: &'a Field,
    metadata: &[u8],
    body: &[u8],
    control: &Control,
) -> Result<CheckedRecursiveBatch<'a>, TypeCodecError> {
    preflight_recursive_record_batch(metadata, body, field, limits(), &verifier(), control)
}
fn read(field: &Field, metadata: &[u8], body: &[u8]) -> RecordBatch {
    let message = ipc::root_as_message(metadata).unwrap();
    ipc::reader::read_record_batch(
        &Buffer::from(body.to_vec()),
        message.header_as_record_batch().unwrap(),
        Arc::new(Schema::new(vec![field.clone()])),
        &HashMap::new(),
        None,
        &message.version(),
    )
    .unwrap()
}
fn prefixes(field: &Field, metadata: &[u8], body: &[u8]) {
    let control = Control::default();
    let _ = checked(field, metadata, body, &control);
    let calls = control.calls.lock().unwrap().clone();
    assert_eq!(calls.first(), Some(&(CompilePhase::Decode, 0)));
    for at in 0..calls.len() {
        for cause in CAUSES {
            let control = Control {
                calls: Mutex::new(Vec::new()),
                stop: Some((at, cause)),
            };
            assert!(
                matches!(checked(field,metadata,body,&control),Err(TypeCodecError::Control(actual)) if actual==cause)
            );
            assert_eq!(*control.calls.lock().unwrap(), calls[..=at]);
        }
    }
}
fn raw(rows: i64, nodes: &[ipc::FieldNode], buffers: &[ipc::Buffer], body_len: i64) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let nodes = builder.create_vector(nodes);
    let buffers = builder.create_vector(buffers);
    let batch = ipc::RecordBatch::create(
        &mut builder,
        &ipc::RecordBatchArgs {
            length: rows,
            nodes: Some(nodes),
            buffers: Some(buffers),
            compression: None,
            variadicBufferCounts: None,
        },
    );
    let message = ipc::Message::create(
        &mut builder,
        &ipc::MessageArgs {
            version: ipc::MetadataVersion::V5,
            header_type: ipc::MessageHeader::RecordBatch,
            header: Some(batch.as_union_value()),
            bodyLength: body_len,
            custom_metadata: None,
        },
    );
    ipc::finish_message_buffer(&mut builder, message);
    builder.finished_data().to_vec()
}

#[test]
fn standard_nested_sliced_struct_lists_and_maps_follow_actual_declaration_geometry() {
    let list = ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 2, 3])),
        Arc::new(Int32Array::from(vec![1, 2, 3])),
        Some(NullBuffer::from(vec![false, true])),
    );
    let array: ArrayRef = Arc::new(StructArray::new(
        vec![
            Arc::new(Field::new("xs", list.data_type().clone(), true)),
            Arc::new(Field::new("label", DataType::Utf8, true)),
        ]
        .into(),
        vec![
            Arc::new(list),
            Arc::new(StringArray::from(vec![Some("hidden"), Some("visible")])),
        ],
        Some(NullBuffer::from(vec![false, true])),
    ));
    for array in [array.clone(), array.slice(1, 1)] {
        let (field, meta, body) = encoded(array.clone());
        let batch = checked(&field, &meta, &body, &Control::default()).unwrap();
        assert_eq!(batch.geometry.field_nodes, 4);
        assert_eq!(batch.nodes[0].children, 2);
        assert_eq!(batch.nodes[1].children, 1);
        assert_eq!(batch.nodes[2].list_map_ancestors, 1);
        assert_eq!(
            batch.nodes[0].subtree_buffer_descriptors,
            batch.geometry.buffer_descriptors
        );
        assert_eq!(
            read(&field, &meta, &body).column(0).to_data(),
            array.to_data()
        );
        prefixes(&field, &meta, &body);
    }
    let entries = StructArray::new(
        vec![
            Arc::new(Field::new("key", DataType::Utf8, false)),
            Arc::new(Field::new("value", DataType::Int64, true)),
        ]
        .into(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(Int64Array::from(vec![Some(1), None])),
        ],
        None,
    );
    let map: ArrayRef = Arc::new(MapArray::new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1, 2])),
        entries,
        None,
        false,
    ));
    let (field, meta, body) = encoded(map.clone());
    let batch = checked(&field, &meta, &body, &Control::default()).unwrap();
    assert_eq!(batch.geometry.field_nodes, 4);
    assert_eq!(
        read(&field, &meta, &body).column(0).to_data(),
        map.to_data()
    );
    prefixes(&field, &meta, &body);
}

#[test]
fn raw_list_nonzero_offsets_and_unused_children_are_legal_but_overruns_refuse() {
    let field = Field::new(
        "xs",
        DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
        true,
    );
    let mut body = Vec::new();
    for value in [1i32, 3, 7, 8, 9, 10] {
        body.extend_from_slice(&value.to_le_bytes());
    }
    let descriptors = [
        ipc::Buffer::new(0, 0),
        ipc::Buffer::new(0, 8),
        ipc::Buffer::new(8, 0),
        ipc::Buffer::new(8, 16),
    ];
    let nodes = [ipc::FieldNode::new(1, 0), ipc::FieldNode::new(4, 0)];
    let meta = raw(1, &nodes, &descriptors, body.len() as i64);
    let geometry = checked(&field, &meta, &body, &Control::default()).unwrap();
    assert_eq!(geometry.geometry.total_rows, 5);
    let decoded = read(&field, &meta, &body);
    let list = decoded
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(list.value_offsets(), &[1, 3]);
    assert_eq!(list.values().len(), 4);
    prefixes(&field, &meta, &body);
    for invalid in [[-1i32, 3], [3, 1], [1, 5]] {
        let mut body = body.clone();
        body[..8].copy_from_slice(
            &invalid
                .into_iter()
                .flat_map(i32::to_le_bytes)
                .collect::<Vec<_>>(),
        );
        assert!(checked(&field, &meta, &body, &Control::default()).is_err());
        prefixes(&field, &meta, &body);
    }
}

#[test]
fn empty_container_geometry_and_parent_null_do_not_bypass_supported_profile() {
    let empty: ArrayRef = Arc::new(StructArray::new_empty_fields(
        3,
        Some(NullBuffer::new_null(3)),
    ));
    let (field, meta, body) = encoded(empty);
    let batch = checked(&field, &meta, &body, &Control::default()).unwrap();
    assert_eq!(batch.geometry.field_nodes, 1);
    assert_eq!(batch.geometry.rows, 3);
    assert_eq!(batch.nodes[0].children, 0);
    assert_eq!(read(&field, &meta, &body).num_rows(), 3);
    prefixes(&field, &meta, &body);
    let field = Field::new(
        "xs",
        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        true,
    );
    let meta = raw(
        0,
        &[ipc::FieldNode::new(0, 0), ipc::FieldNode::new(0, 0)],
        &[
            ipc::Buffer::new(0, 0),
            ipc::Buffer::new(0, 0),
            ipc::Buffer::new(0, 0),
            ipc::Buffer::new(0, 0),
        ],
        0,
    );
    assert!(checked(&field, &meta, &[], &Control::default()).is_ok());
    assert_eq!(read(&field, &meta, &[]).num_rows(), 0);
    prefixes(&field, &meta, &[]);
    let unsupported = Field::new(
        "xs",
        DataType::Struct(
            vec![Arc::new(Field::new(
                "unsupported",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, true)), 0),
                true,
            ))]
            .into(),
        ),
        true,
    );
    let meta = raw(
        0,
        &[ipc::FieldNode::new(0, 0), ipc::FieldNode::new(0, 0)],
        &[ipc::Buffer::new(0, 0)],
        0,
    );
    assert!(checked(&unsupported, &meta, &[], &Control::default()).is_err());
    prefixes(&unsupported, &meta, &[]);
}

#[test]
fn recursive_geometry_refuses_node_buffer_row_and_scratch_envelopes_with_original_causes() {
    let fields = (0..320)
        .map(|i| Arc::new(Field::new(format!("c{i}"), DataType::Int32, false)))
        .collect::<Vec<_>>();
    let arrays = (0..320)
        .map(|_| Arc::new(Int32Array::from(vec![1])) as ArrayRef)
        .collect::<Vec<_>>();
    let (field, meta, body) = encoded(Arc::new(StructArray::new(fields.into(), arrays, None)));
    let control = Control::default();
    let actual = checked(&field, &meta, &body, &control).unwrap();
    assert!(
        control
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|(_, units)| *units == 256)
    );
    assert_eq!(actual.nodes.len(), 321);
    prefixes(&field, &meta, &body);
    for constraint in 0..5 {
        let mut cap = limits();
        match constraint {
            0 => cap.max_field_nodes = 320,
            1 => cap.max_total_rows = 320,
            2 => cap.max_geometry_request_bytes = actual.scratch_request_bytes - 1,
            3 => cap.flat.max_buffer_descriptors = actual.geometry.buffer_descriptors - 1,
            _ => cap.flat.max_rows = 0,
        };
        assert!(
            preflight_recursive_record_batch(
                &meta,
                &body,
                &field,
                cap,
                &verifier(),
                &Control::default()
            )
            .is_err()
        );
    }
}
