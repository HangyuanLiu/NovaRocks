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

//! Original observed List CAST invocation: Null child lift and exact Field retag.
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::{ExprArena, ExprNode, cast_array_to_target};
use arrow::array::{Array, ArrayRef, ListArray, NullArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_buffer::{NullBuffer, OffsetBuffer};
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType};
use novarocks_types::SlotId;
use std::{collections::HashMap, sync::Arc};
pub(super) fn source_type(null_child: bool) -> DataType {
    DataType::List(Arc::new(Field::new(
        "",
        if null_child {
            DataType::Null
        } else {
            DataType::Utf8
        },
        true,
    )))
}
pub(super) fn target_type(null_child: bool, field_id: Option<&str>) -> DataType {
    let field = Field::new(
        if field_id.is_some() { "element" } else { "" },
        if null_child {
            DataType::Int32
        } else {
            DataType::Utf8
        },
        true,
    );
    DataType::List(Arc::new(if let Some(id) = field_id {
        field.with_metadata(HashMap::from([("PARQUET:field_id".into(), id.into())]))
    } else {
        field
    }))
}
pub(super) fn input(null_child: bool) -> ArrayRef {
    let DataType::List(field) = source_type(null_child) else {
        unreachable!()
    };
    let values: ArrayRef = if null_child {
        Arc::new(NullArray::new(5))
    } else {
        Arc::new(StringArray::from(vec![
            Some("beijing"),
            Some("shanghai"),
            None,
            Some("字\0x"),
            Some(""),
        ]))
    };
    Arc::new(ListArray::new(
        field,
        OffsetBuffer::new(vec![0_i32, 2, 2, 3, 5].into()),
        values,
        Some(NullBuffer::from(vec![true, true, false, true])),
    ))
}
pub(super) fn modes() -> [(DecimalOverflowPolicy, bool); 4] {
    [
        (DecimalOverflowPolicy::OutputNull, false),
        (DecimalOverflowPolicy::OutputNull, true),
        (DecimalOverflowPolicy::ReportError, false),
        (DecimalOverflowPolicy::ReportError, true),
    ]
}
pub(super) fn actual(
    array: ArrayRef,
    target: DataType,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> Result<ArrayRef, String> {
    let ty = FunctionValueType::new(array.data_type().clone(), true);
    let data = RecordBatch::try_new(
        Arc::new(Schema::new(vec![ty.try_to_field("original-list").unwrap()])),
        vec![array],
    )
    .unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(data.schema().as_ref(), &[SlotId::new(17)])
            .unwrap();
    let mut arena = ExprArena::default();
    arena.set_allow_throw_exception(allow);
    let child = arena.push_typed(ExprNode::SlotId(SlotId::new(17)), ty.data_type);
    let root = arena.push_typed(ExprNode::Cast(child, policy), target);
    arena.eval(root, &Chunk::new_with_chunk_schema(data, schema))
}
pub(super) fn same_values(left: &ArrayRef, right: &ArrayRef) {
    assert_eq!(left.data_type(), right.data_type());
    assert_eq!(left.len(), right.len());
    for row in 0..left.len() {
        assert_eq!(left.is_null(row), right.is_null(row));
        if !left.is_null(row) {
            let left = left
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value(row);
            let right = right
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value(row);
            assert_eq!(left.to_data(), right.to_data());
        }
    }
}
#[test]
fn observed_list_cast_original_null_to_int32_complete_offsets_null_empty_and_policy() {
    let source = input(true);
    let old = source.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(
        old.values().null_count(),
        0,
        "Arrow58 physical Null child is not an all-null bitmap"
    );
    assert_eq!(old.values().logical_null_count(), 5);
    let target = target_type(true, None);
    for (policy, allow) in modes() {
        let out = actual(source.clone(), target.clone(), policy, allow).unwrap();
        let list = out.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list.data_type(), &target);
        assert_eq!(list.value_offsets(), old.value_offsets());
        assert_eq!(list.nulls(), old.nulls());
        assert_eq!(list.values().data_type(), &DataType::Int32);
        assert_eq!(list.values().null_count(), 5);
        assert_eq!(list.value_length(0), 2);
        assert_eq!(list.value_length(1), 0);
        assert!(list.is_null(2));
        assert_eq!(list.value_length(3), 2);
    }
}
fn retag(id: &str) {
    let source = input(false);
    let original = source.as_any().downcast_ref::<ListArray>().unwrap();
    let target = target_type(false, Some(id));
    for (policy, allow) in modes() {
        let out = actual(source.clone(), target.clone(), policy, allow).unwrap();
        let out = out.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(out.data_type(), &target);
        assert!(Arc::ptr_eq(out.values(), original.values()));
        assert_eq!(
            out.value_offsets().as_ptr(),
            original.value_offsets().as_ptr()
        );
        assert_eq!(out.nulls(), original.nulls());
        let DataType::List(field) = out.data_type() else {
            unreachable!()
        };
        assert_eq!(field.name(), "element");
        assert_eq!(field.metadata().get("PARQUET:field_id").unwrap(), id);
        assert!(field.is_nullable());
    }
}
#[test]
fn observed_list_cast_original_utf8_field_id7_retags_without_value_reinterpretation() {
    retag("7");
}
#[test]
fn observed_list_cast_original_utf8_field_id6_retags_without_value_reinterpretation() {
    retag("6");
}
#[test]
fn observed_list_cast_original_slices_nonzero_offsets_hidden_null_and_empty_metadata() {
    for (null, id) in [(true, None), (false, Some("6")), (false, Some("7"))] {
        let source = input(null);
        let target = target_type(null, id);
        for source in [source.slice(1, 3), source.slice(2, 1), source.slice(1, 0)] {
            let original = source.as_any().downcast_ref::<ListArray>().unwrap();
            for (policy, allow) in modes() {
                let out = actual(source.clone(), target.clone(), policy, allow).unwrap();
                let list = out.as_any().downcast_ref::<ListArray>().unwrap();
                assert_eq!(list.data_type(), &target);
                assert_eq!(list.value_offsets(), original.value_offsets());
                assert_eq!(list.nulls(), original.nulls());
                assert_eq!(list.values().len(), original.values().len());
            }
        }
    }
}
#[test]
fn observed_list_cast_original_project_and_arena_share_observed_profile_values() {
    for (null, id) in [(true, None), (false, Some("6")), (false, Some("7"))] {
        let source = input(null);
        let target = target_type(null, id);
        let project = cast_array_to_target(&source, &target).unwrap();
        for (policy, allow) in modes() {
            same_values(
                &actual(source.clone(), target.clone(), policy, allow).unwrap(),
                &project,
            );
        }
    }
}
#[test]
fn observed_list_cast_original_exact_dtype_remains_original_arc_identity() {
    for null in [false, true] {
        let source = input(null);
        for (policy, allow) in modes() {
            let out = actual(source.clone(), source.data_type().clone(), policy, allow).unwrap();
            assert!(Arc::ptr_eq(&source, &out));
        }
    }
}
#[test]
fn observed_list_cast_original_nonnullable_child_constructor_panic_is_preserved() {
    let target = DataType::List(Arc::new(Field::new("element", DataType::Int32, false)));
    for (policy, allow) in modes() {
        let panic = std::panic::catch_unwind(|| actual(input(true), target.clone(), policy, allow))
            .unwrap_err();
        let text = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied())
            .unwrap();
        assert_eq!(
            text,
            "called `Result::unwrap()` on an `Err` value: InvalidArgumentError(\"Non-nullable field of ListArray \\\"element\\\" cannot contain nulls\")"
        );
    }
}
