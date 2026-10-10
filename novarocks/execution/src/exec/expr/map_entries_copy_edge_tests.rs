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
//! Original zero-copy Map entries accepts a valid oversized retained Int8 dictionary.
use super::{ExprArena, ExprId, ExprNode};
use crate::exec::chunk::{Chunk, ChunkSchema};
use arrow::array::{
    Array, ArrayRef, DictionaryArray, Int8Array, Int32Array, ListArray, MapArray, StringArray,
    StructArray, types::Int8Type,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_buffer::OffsetBuffer;
use novarocks_types::SlotId;
use std::sync::Arc;
fn fixture() -> ArrayRef {
    let dict = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![0, 0]),
        Arc::new(StringArray::from(
            (0..128)
                .map(|i| format!("original-{i}"))
                .collect::<Vec<_>>(),
        )),
    )
    .unwrap();
    let values: ArrayRef = Arc::new(dict);
    let entries = StructArray::new(
        vec![
            Arc::new(Field::new("key", DataType::Int32, false)),
            Arc::new(Field::new("value", values.data_type().clone(), true)),
        ]
        .into(),
        vec![Arc::new(Int32Array::from(vec![1, 1])), values],
        None,
    );
    Arc::new(MapArray::new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::new(vec![0, 2].into()),
        entries,
        None,
        false,
    ))
}
#[test]
fn original_map_entries_valid_dictionary128_is_shared_without_copy_constructor_limit() {
    let a = fixture();
    let slot = SlotId::new(1);
    let dtype = a.data_type().clone();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("input", dtype.clone(), false)])),
        vec![a],
    )
    .unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[slot]).unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, schema);
    let mut arena = ExprArena::default();
    let arg = arena.push_typed(ExprNode::SlotId(slot), dtype);
    let out =
        super::function::map::eval_map_entries(&arena, ExprId(usize::MAX), &[arg], &chunk).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    let values = list
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
        .column(1)
        .as_any()
        .downcast_ref::<DictionaryArray<Int8Type>>()
        .unwrap();
    assert_eq!(values.values().len(), 128);
    assert_eq!(values.keys().values(), &[0, 0]);
    assert_eq!(out.len(), 1);
}
#[test]
fn pure_differential_map_entries_valid_dictionary128_copy_edge_requires_original_equality() {
    super::pure_differential::assert_scalar_matches_v1(
        super::pure_differential::ScalarDiffSpec::new("map_entries")
            .column(fixture())
            .sparse_selections(2, 778),
    );
}
