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

//! Independent original ExprArena constructor and raw map access baselines.
use super::{ExprArena, ExprId, ExprNode};
use crate::exec::chunk::{Chunk, ChunkSchema};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, ListArray, MapArray, NullArray,
    StringArray, StructArray, new_empty_array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_buffer::{NullBuffer, OffsetBuffer};
use novarocks_types::SlotId;
use std::{collections::HashMap, sync::Arc};
fn setup(mut columns: Vec<ArrayRef>, rows: usize) -> (ExprArena, Vec<ExprId>, Chunk) {
    if columns.is_empty() {
        columns.push(Arc::new(Int32Array::from(vec![0; rows])))
    }
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
        .collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
    (arena, ids, Chunk::new_with_chunk_schema(batch, schema))
}
fn array(
    columns: Vec<ArrayRef>,
    rows: usize,
    output: Option<DataType>,
) -> Result<ArrayRef, String> {
    let empty = columns.is_empty();
    let (mut arena, mut ids, chunk) = setup(columns, rows);
    if empty {
        ids.clear()
    }
    let node = ExprNode::ArrayExpr {
        elements: ids.clone(),
    };
    let id = match output {
        Some(t) => arena.push_typed(node, t),
        None => arena.push(node),
    };
    super::array_expr::eval_array_expr(&arena, id, &ids, &chunk)
}
fn map(keys: ArrayRef, values: ArrayRef, offsets: Vec<i32>, valid: Option<Vec<bool>>) -> ArrayRef {
    let fs = vec![
        Arc::new(Field::new("key", keys.data_type().clone(), true)),
        Arc::new(Field::new("value", values.data_type().clone(), true)),
    ];
    let entries = StructArray::new(fs.into(), vec![keys, values], None);
    Arc::new(MapArray::new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::new(offsets.into()),
        entries,
        valid.map(NullBuffer::from),
        false,
    ))
}
fn lookup(columns: Vec<ArrayRef>, output: Option<DataType>) -> Result<ArrayRef, String> {
    let rows = columns[0].len();
    let (mut arena, ids, chunk) = setup(columns, rows);
    let id = match output {
        Some(t) => arena.push_typed(ExprNode::SlotId(SlotId::new(999)), t),
        None => ExprId(usize::MAX),
    };
    super::function::map::eval_element_at(&arena, id, &ids, &chunk)
}
fn ints(a: &ArrayRef) -> Vec<Option<i32>> {
    a.as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .iter()
        .collect()
}

#[test]
fn original_map_null_result_miss_panics_with_exact_nullarray_bitmap_message() {
    let m = map(
        Arc::new(Int32Array::from(vec![1])),
        Arc::new(NullArray::new(1)),
        vec![0, 1],
        None,
    );
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        lookup(vec![m, Arc::new(Int32Array::from(vec![2]))], None)
    }))
    .expect_err("original apply_indices_nulls overlays NullArray bitmap");
    let message = panic
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap();
    assert_eq!(
        message,
        "NullArray data should not contain a null buffer, as no buffers are required"
    );
}
