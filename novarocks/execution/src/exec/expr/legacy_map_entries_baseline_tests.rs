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
//! Independent original MAP_ENTRIES contracts before extraction.
use super::{ExprArena, ExprId, ExprNode};
use crate::exec::chunk::{Chunk, ChunkSchema};
use arrow::array::{Array, ArrayRef, Int32Array, ListArray, MapArray, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_buffer::{NullBuffer, OffsetBuffer};
use novarocks_types::SlotId;
use std::sync::Arc;

pub(super) fn map(
    keys: ArrayRef,
    values: ArrayRef,
    offsets: Vec<i32>,
    valid: Option<Vec<bool>>,
    sorted: bool,
) -> ArrayRef {
    let entries = StructArray::new(
        vec![
            Arc::new(
                Field::new("authored-key", keys.data_type().clone(), false)
                    .with_metadata([("key-source".into(), "preserved".into())].into()),
            ),
            Arc::new(
                Field::new("authored-value", values.data_type().clone(), true)
                    .with_metadata([("value-source".into(), "preserved".into())].into()),
            ),
        ]
        .into(),
        vec![keys, values],
        None,
    );
    Arc::new(MapArray::new(
        Arc::new(
            Field::new("authored-entries", entries.data_type().clone(), false)
                .with_metadata([("entry-source".into(), "preserved".into())].into()),
        ),
        OffsetBuffer::new(offsets.into()),
        entries,
        valid.map(NullBuffer::from),
        sorted,
    ))
}
pub(super) fn fixture(sorted: bool) -> ArrayRef {
    map(
        Arc::new(Int32Array::from(vec![9, 1, 9, 4, 7])),
        Arc::new(StringArray::from(vec![
            Some("九\0"),
            None,
            Some("last"),
            Some("hidden"),
            Some("七"),
        ])),
        vec![0, 3, 3, 4, 5],
        Some(vec![true, true, false, true]),
        sorted,
    )
}
fn setup(a: ArrayRef) -> (ExprArena, Vec<ExprId>, Chunk) {
    let mut arena = ExprArena::default();
    let slot = SlotId::new(1);
    let id = arena.push_typed(ExprNode::SlotId(slot), a.data_type().clone());
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "source",
            a.data_type().clone(),
            true,
        )])),
        vec![a],
    )
    .unwrap();
    let cs =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[slot]).unwrap();
    (arena, vec![id], Chunk::new_with_chunk_schema(batch, cs))
}
fn raw(a: ArrayRef, target: Option<DataType>) -> Result<ArrayRef, String> {
    let (mut arena, args, chunk) = setup(a);
    let expr = target
        .map(|t| arena.push_typed(ExprNode::SlotId(SlotId::new(999)), t))
        .unwrap_or(ExprId(usize::MAX));
    super::function::map::eval_map_entries(&arena, expr, &args, &chunk)
}
#[test]
fn original_map_entries_offsets_duplicates_order_nulls_and_fields_are_unchanged() {
    for sorted in [false, true] {
        let a = fixture(sorted);
        let map = a.as_any().downcast_ref::<MapArray>().unwrap();
        let out = raw(a.clone(), None).unwrap();
        let list = out.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list.value_offsets(), map.value_offsets());
        assert_eq!(list.nulls(), map.nulls());
        assert_eq!(list.values().to_data(), map.entries().to_data());
        assert_eq!(
            out.data_type(),
            &DataType::List(Arc::new(Field::new(
                "item",
                map.entries().data_type().clone(),
                true
            )))
        );
        assert_eq!(list.value_length(0), 3);
        assert!(list.is_null(2));
        assert_eq!(
            list.values()
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[9, 1, 9, 4, 7]
        );
    }
}
#[test]
fn original_map_entries_slices_empty_and_hidden_null_entries_share_original_carriers() {
    let a = fixture(false);
    for a in [a.slice(1, 3), a.slice(0, 0)] {
        let map = a.as_any().downcast_ref::<MapArray>().unwrap();
        let out = raw(a.clone(), None).unwrap();
        let list = out.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list.len(), map.len());
        assert_eq!(list.value_offsets(), map.value_offsets());
        assert_eq!(list.values().to_data(), map.entries().to_data());
        assert_eq!(list.nulls(), map.nulls());
    }
}
#[test]
fn original_map_entries_nested_children_are_not_decoded_or_sorted() {
    let child: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("nested", DataType::Int32, true)),
        OffsetBuffer::new(vec![0, 1, 2].into()),
        Arc::new(Int32Array::from(vec![None, Some(8)])),
        None,
    ));
    let a = map(
        Arc::new(Int32Array::from(vec![2, 1])),
        child,
        vec![0, 2],
        None,
        false,
    );
    let out = raw(a.clone(), None).unwrap();
    assert_eq!(
        out.as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .values()
            .to_data(),
        a.as_any()
            .downcast_ref::<MapArray>()
            .unwrap()
            .entries()
            .to_data()
    );
}
#[test]
fn original_map_entries_bad_source_and_output_cast_keep_full_original_errors() {
    assert_eq!(
        raw(Arc::new(Int32Array::from(vec![1])), None).unwrap_err(),
        "map_entries expects MapArray, got Int32"
    );
    let a = fixture(false);
    let expected =
        arrow::compute::cast(&raw(a.clone(), None).unwrap(), &DataType::Int32).unwrap_err();
    assert_eq!(
        raw(a, Some(DataType::Int32)).unwrap_err(),
        format!("map_entries: failed to cast output: {expected}")
    );
}
#[test]
fn original_map_entries_raw_empty_arity_panics_before_any_output_lookup() {
    let (arena, _, chunk) = setup(fixture(false));
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            super::function::map::eval_map_entries(&arena, ExprId(usize::MAX), &[], &chunk)
        }))
        .is_err()
    );
}

#[test]
fn original_map_entries_first_child_error_and_extra_argument_not_demanded() {
    let a = fixture(false);
    let (arena, mut args, chunk) = setup(a.clone());
    args.push(ExprId(usize::MAX));
    let out =
        super::function::map::eval_map_entries(&arena, ExprId(usize::MAX), &args, &chunk).unwrap();
    assert_eq!(out.to_data(), raw(a, None).unwrap().to_data());
    assert_eq!(
        super::function::map::eval_map_entries(
            &arena,
            ExprId(usize::MAX),
            &[ExprId(usize::MAX)],
            &chunk
        )
        .unwrap_err(),
        "invalid ExprId"
    );
}
#[test]
fn original_map_entries_constant_keeps_original_host_broadcast() {
    let a = fixture(false).slice(0, 1);
    let constant = super::pure_differential::constant(
        novarocks_type_contract::FunctionValueType::new(a.data_type().clone(), true),
        a.clone(),
    );
    let (mut arena, _, chunk) = setup(fixture(false));
    let id = arena.push_typed(ExprNode::Constant(constant), a.data_type().clone());
    let out =
        super::function::map::eval_map_entries(&arena, ExprId(usize::MAX), &[id], &chunk).unwrap();
    assert_eq!(chunk.len(), 4);
    assert_eq!(out.len(), chunk.len());
    let original_broadcast = arena.eval(id, &chunk).unwrap();
    assert_eq!(original_broadcast.len(), chunk.len());
    assert_eq!(
        out.to_data(),
        raw(original_broadcast, None).unwrap().to_data()
    );
}
