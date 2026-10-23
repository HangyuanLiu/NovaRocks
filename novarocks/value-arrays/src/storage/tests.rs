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

use super::*;
use arrow_array::types::{Int8Type, Int16Type};
use arrow_array::{
    ArrayRef, DictionaryArray, Int8Array, Int16Array, Int32Array, ListArray, ListViewArray,
    RunArray, StringArray, StructArray, UnionArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{Field, UnionFields};
use std::convert::Infallible;
use std::sync::Arc;

#[test]
fn region_slice_geometry_preserves_accessibility_and_unknown_owner() {
    let buffer = Buffer::from_vec((0u8..32).collect::<Vec<_>>());
    let slice = buffer.slice_with_length(8, 16);
    let region = BufferRegion::new(slice);
    assert_eq!(
        region.owner(),
        OwnerFacts {
            source: None,
            full_capacity: None
        }
    );
    assert_eq!(region.bytes(), &(8u8..24).collect::<Vec<_>>());
    let facts = region.facts();
    assert_eq!(facts.base_address, buffer.data_ptr().as_ptr() as usize);
    assert_eq!(facts.offset, 8);
    assert_eq!(facts.accessible_bytes, 16);
    assert_eq!(facts.accessible_range(), 8..24);
    assert_eq!(facts.declared_capacity, buffer.capacity());
    assert_eq!(facts.owner.full_capacity, None);
    let sub = region.slice(3..9).unwrap();
    assert_eq!(sub.bytes(), &[11, 12, 13, 14, 15, 16]);
    assert_eq!(sub.facts().accessible_range(), 11..17);
    assert!(matches!(
        region.slice(17..17),
        Err(ValueError::Invariant(ValueViolation::Bounds))
    ));
    assert!(matches!(
        region.slice(9..8),
        Err(ValueError::Invariant(ValueViolation::Bounds))
    ));
    assert_eq!(region.slice(16..16).unwrap().len(), 0);
}

#[test]
fn knowledgeable_owner_facts_remain_explicit_across_slices() {
    let region = BufferRegion::with_owner(
        Buffer::from_vec(vec![1u8; 24]),
        OwnerFacts {
            source: Some("known-test-owner"),
            full_capacity: Some(4096),
        },
    );
    let sub = region.slice(2..9).unwrap();
    assert_eq!(sub.owner(), region.owner());
    assert_eq!(sub.facts().accessible_bytes, 7);
    assert_eq!(sub.facts().owner.full_capacity, Some(4096));
    // A declared Arrow capacity and an owner-supplied capacity are separate
    // premises; the former is never promoted into the latter by inspection.
    assert_ne!(sub.facts().declared_capacity, 4096);
}

#[test]
fn physical_visitor_keeps_null_parent_slice_external_and_unreferenced_values() {
    let strings: ArrayRef = Arc::new(StringArray::from(vec![
        "unreferenced dictionary payload",
        "used",
    ]));
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![1, 1, 1]), strings).unwrap(),
    );
    let child_type = dictionary.data_type().clone();
    let list: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", child_type, false)),
        OffsetBuffer::new(vec![0i32, 1, 2, 3].into()),
        dictionary,
        Some(NullBuffer::from(vec![true, false, true])),
    ));
    let root = list.slice(1, 1);
    assert!(root.is_null(0));
    let mut nodes = Vec::new();
    visit_borrowed(root.as_ref(), &mut |node| {
        nodes.push((
            node.data_type().clone(),
            node.data_buffers(),
            node.child_count(),
            match node {
                BorrowedNode::Array { array, .. } => array.len(),
                BorrowedNode::RunEnds { .. } => unreachable!(),
            },
        ));
        Ok::<_, Infallible>(())
    })
    .unwrap();
    assert_eq!(nodes.len(), 3);
    assert_eq!(nodes[0], (DataType::Utf8, 2, 0, 2));
    assert!(matches!(nodes[1].0, DataType::Dictionary(_, _)));
    assert_eq!(nodes[1].3, 3);
    assert!(matches!(nodes[2].0, DataType::List(_)));
    assert_eq!(nodes[2].3, 1);
    let mut buffers = Vec::new();
    visit_buffers(root.as_ref(), &mut |buffer| {
        buffers.push(buffer.as_slice().to_vec());
        Ok::<_, Infallible>(())
    })
    .unwrap();
    assert_eq!(buffers.len(), 5); // two string + keys + list offsets + parent bitmap
    assert!(
        buffers
            .iter()
            .any(|b| b == b"unreferenced dictionary payloadused")
    );
}

#[test]
fn generic_structure_keeps_list_view_union_and_all_arrow_dictionary_keys() {
    let child: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 30, 40]));
    let list_view = ListViewArray::try_new(
        Arc::new(Field::new("item", DataType::Int32, false)),
        ScalarBuffer::from(vec![2i32, 0]),
        ScalarBuffer::from(vec![2i32, 1]),
        child,
        None,
    )
    .unwrap();
    let root = borrowed_node(&list_view).unwrap();
    assert_eq!((root.data_buffers(), root.child_count()), (2, 1));
    assert_eq!(
        borrowed_child(&list_view, 0).unwrap().data_type(),
        &DataType::Int32
    );
    assert!(borrowed_child(&list_view, 1).is_err());
    let fields = UnionFields::try_new(
        [3, 7],
        [
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ],
    )
    .unwrap();
    let union = UnionArray::try_new(
        fields,
        vec![3i8, 7].into(),
        Some(vec![0i32, 0].into()),
        vec![
            Arc::new(Int32Array::from(vec![10])) as ArrayRef,
            Arc::new(StringArray::from(vec!["payload"])),
        ],
    )
    .unwrap();
    let mut types = Vec::new();
    visit_borrowed(&union, &mut |node| {
        types.push(node.data_type().clone());
        Ok::<_, Infallible>(())
    })
    .unwrap();
    assert_eq!(types.len(), 3);
    assert_eq!(types[0], DataType::Int32);
    assert_eq!(types[1], DataType::Utf8);
    assert_eq!(borrowed_node(&union).unwrap().data_buffers(), 2);
    let dictionary = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![0]),
        Arc::new(Int32Array::from(vec![99])),
    )
    .unwrap();
    assert_eq!(borrowed_node(&dictionary).unwrap().child_count(), 1);
    assert_eq!(
        borrowed_child(&dictionary, 0).unwrap().data_type(),
        &DataType::Int32
    );
}

#[test]
fn run_ends_borrow_the_real_buffer_without_a_temporary_array() {
    let ends = Int16Array::from(vec![2, 5]);
    let values = StringArray::from(vec!["first", "second"]);
    let run = RunArray::<Int16Type>::try_new(&ends, &values).unwrap();
    let mut nodes = Vec::new();
    visit_borrowed(&run, &mut |node| {
        nodes.push((
            node.data_type().clone(),
            node.data_buffers(),
            node.child_count(),
        ));
        if let BorrowedNode::RunEnds { buffer, .. } = node {
            assert_eq!(buffer.as_ptr(), run.run_ends().inner().inner().as_ptr());
        }
        Ok::<_, Infallible>(())
    })
    .unwrap();
    assert_eq!(nodes[0], (DataType::Int16, 1, 0));
    assert_eq!(nodes[1], (DataType::Utf8, 2, 0));
    assert!(matches!(nodes[2].0, DataType::RunEndEncoded(_, _)));
    assert_eq!((nodes[2].1, nodes[2].2), (0, 2));
    let mut buffers = Vec::new();
    visit_buffers(&run, &mut |buffer| {
        buffers.push(buffer.as_ptr());
        Ok::<_, Infallible>(())
    })
    .unwrap();
    assert_eq!(buffers.len(), 3);
    assert_eq!(buffers[0], run.run_ends().inner().inner().as_ptr());
}

#[test]
fn visitor_returns_the_same_observer_cause_and_stops_before_parent() {
    let fields = vec![
        Arc::new(Field::new("x", DataType::Int32, false)),
        Arc::new(Field::new("y", DataType::Int32, false)),
    ];
    let children = vec![
        Arc::new(Int32Array::from(vec![1])) as ArrayRef,
        Arc::new(Int32Array::from(vec![2])),
    ];
    let root = StructArray::new(fields.into(), children, None);
    let marker = Arc::new(43);
    let mut calls = 0;
    let error = visit_borrowed(&root, &mut |_| {
        calls += 1;
        Err(marker.clone())
    })
    .unwrap_err();
    let ValueFailure::Caller(cause) = error else {
        panic!("original caller cause")
    };
    assert!(Arc::ptr_eq(&cause, &marker));
    assert_eq!(calls, 1);
    let error = visit_buffers(&root, &mut |_| Err(marker.clone())).unwrap_err();
    let ValueFailure::Caller(cause) = error else {
        panic!("original buffer observer cause")
    };
    assert!(Arc::ptr_eq(&cause, &marker));
}
