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

//! Original array_append raw contracts, no pure implementation involved.
use super::{ExprArena, ExprId, ExprNode};
use crate::exec::chunk::{Chunk, ChunkSchema};
use arrow::array::{Array, ArrayRef, ListArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_buffer::{NullBuffer, OffsetBuffer};
use novarocks_types::SlotId;
use std::sync::Arc;
fn list(
    field: Arc<Field>,
    values: ArrayRef,
    offsets: Vec<i32>,
    valid: Option<Vec<bool>>,
) -> ArrayRef {
    Arc::new(ListArray::new(
        field,
        OffsetBuffer::new(offsets.into()),
        values,
        valid.map(NullBuffer::from),
    ))
}
fn raw(columns: Vec<ArrayRef>, output: Option<DataType>) -> Result<ArrayRef, String> {
    let slots = (0..columns.len())
        .map(|i| SlotId::new(i as u32 + 1))
        .collect::<Vec<_>>();
    let fields = columns
        .iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("v{i}"), a.data_type().clone(), true))
        .collect::<Vec<_>>();
    let mut arena = ExprArena::default();
    let ids = columns
        .iter()
        .enumerate()
        .map(|(i, a)| arena.push_typed(ExprNode::SlotId(slots[i]), a.data_type().clone()))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, schema);
    let id = match output {
        Some(t) => arena.push_typed(ExprNode::SlotId(SlotId::new(999)), t),
        None => ExprId(usize::MAX),
    };
    super::function::array::eval_array_append(&arena, id, &ids, &chunk)
}
fn dictionary(text: &str, count: usize) -> ArrayRef {
    use arrow::array::{DictionaryArray, Int8Array};
    use arrow::datatypes::Int8Type;
    Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![0]),
            Arc::new(StringArray::from(vec![text; count])),
        )
        .unwrap(),
    )
}
#[test]
fn original_append_dictionary_full_backing_constructor_panics_before_null_parent_loop() {
    let child = dictionary("first", 64);
    let a = list(
        Arc::new(Field::new("item", child.data_type().clone(), true)),
        child,
        vec![0, 1],
        Some(vec![false]),
    );
    let target = dictionary("second", 64);
    let panic =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| raw(vec![a, target], None)))
            .err()
            .unwrap();
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap();
    assert_eq!(
        message,
        "MutableArrayData::new is infallible: DictionaryKeyOverflowError"
    );
}
#[test]
fn original_append_dictionary_unused_full_domains_and_view_buffers_are_kept() {
    use arrow::array::{DictionaryArray, StringViewArray};
    use arrow::datatypes::Int8Type;
    let child = dictionary("first", 3);
    let a = list(
        Arc::new(Field::new("item", child.data_type().clone(), true)),
        child,
        vec![0, 1],
        None,
    );
    let out = raw(vec![a, dictionary("second", 2)], None).unwrap();
    let out = out.as_any().downcast_ref::<ListArray>().unwrap();
    let out = out
        .values()
        .as_any()
        .downcast_ref::<DictionaryArray<Int8Type>>()
        .unwrap();
    assert_eq!(out.values().len(), 5);
    assert_eq!(out.keys().values().as_ref(), &[0, 3]);
    let child: ArrayRef = Arc::new(StringViewArray::from(vec![Some(
        "child-original-long-view",
    )]));
    let a = list(
        Arc::new(Field::new("item", child.data_type().clone(), true)),
        child,
        vec![0, 1],
        None,
    );
    let target: ArrayRef = Arc::new(StringViewArray::from(vec![Some(
        "target-original-long-view",
    )]));
    let out = raw(vec![a, target], None).unwrap();
    let out = out.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(
        out.values()
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![
            Some("child-original-long-view"),
            Some("target-original-long-view")
        ]
    );
    assert_eq!(out.values().to_data().buffers().len(), 3);
}
