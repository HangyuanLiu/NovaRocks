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
//! Independent original raw string-family baselines; no pure owner needed.
use super::{ExprArena, ExprId, ExprNode};
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::eval_string_function;
use crate::runtime::fragment::ExecutionResult;
use arrow::array::{
    Array, ArrayRef, BooleanArray, Int64Array, LargeStringArray, ListArray, NullArray, StringArray,
    StructArray, new_null_array,
};
use arrow::array::{Int32Builder, ListBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_failure_contract::{FailureClass, FailureProjection};
use novarocks_types::SlotId;
use std::sync::Arc;
fn setup(columns: Vec<ArrayRef>) -> (ExprArena, Vec<ExprId>, Chunk) {
    let fields = columns
        .iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("v{i}"), a.data_type().clone(), true))
        .collect::<Vec<_>>();
    let slots = (1..=columns.len())
        .map(|i| SlotId::new(i as u32))
        .collect::<Vec<_>>();
    let mut arena = ExprArena::default();
    let args = columns
        .iter()
        .enumerate()
        .map(|(i, a)| arena.push_typed(ExprNode::SlotId(slots[i]), a.data_type().clone()))
        .collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
    (arena, args, Chunk::new_with_chunk_schema(batch, schema))
}
fn raw(name: &str, columns: Vec<ArrayRef>) -> ExecutionResult<ArrayRef> {
    let (arena, args, chunk) = setup(columns);
    eval_string_function(name, &arena, ExprId(usize::MAX), &args, &chunk)
}
fn text(v: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(v))
}
fn parts(output: &ArrayRef) -> Vec<Option<Vec<String>>> {
    let list = output.as_any().downcast_ref::<ListArray>().unwrap();
    (0..list.len())
        .map(|row| {
            if list.is_null(row) {
                None
            } else {
                let child = list.value(row);
                Some(
                    child
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .iter()
                        .map(|s| s.unwrap().to_owned())
                        .collect(),
                )
            }
        })
        .collect()
}
fn bools(output: &ArrayRef) -> Vec<Option<bool>> {
    output
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}

#[test]
fn original_null_or_empty_utf8_null_unicode_nul_and_slices() {
    let a = text(vec![None, Some(""), Some("é"), Some("\0"), Some(" ")]);
    assert_eq!(
        bools(&raw("null_or_empty", vec![a.clone()]).unwrap()),
        vec![
            Some(true),
            Some(true),
            Some(false),
            Some(false),
            Some(false)
        ]
    );
    assert_eq!(
        bools(&raw("null_or_empty", vec![a.slice(1, 3)]).unwrap()),
        vec![Some(true), Some(false), Some(false)]
    );
}
#[test]
fn original_null_or_empty_list_parent_null_empty_and_null_child_counted() {
    let mut b = ListBuilder::new(Int32Builder::new());
    b.append(false);
    b.append(true);
    b.values().append_null();
    b.append(true);
    b.values().append_value(7);
    b.append(true);
    assert_eq!(
        bools(&raw("null_or_empty", vec![Arc::new(b.finish())]).unwrap()),
        vec![Some(true), Some(true), Some(false), Some(false)]
    );
}
#[test]
fn original_null_or_empty_null_and_typed_all_null_unsupported_are_true() {
    for ty in [
        DataType::Null,
        DataType::Int64,
        DataType::LargeUtf8,
        DataType::Boolean,
    ] {
        let a = new_null_array(&ty, 3);
        assert_eq!(
            bools(&raw("null_or_empty", vec![a]).unwrap()),
            vec![Some(true); 3]
        );
        assert_eq!(
            raw("null_or_empty", vec![new_null_array(&ty, 0)])
                .unwrap()
                .len(),
            0
        );
    }
}
#[test]
fn original_null_or_empty_mixed_unsupported_errors_before_row_projection() {
    let a: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(1)]));
    assert_eq!(
        raw("null_or_empty", vec![a.clone()])
            .unwrap_err()
            .to_string(),
        "null_or_empty expects string or array, got Int64"
    );
    assert_eq!(
        bools(&raw("null_or_empty", vec![a.slice(0, 1)]).unwrap()),
        vec![Some(true)]
    );
    assert_eq!(
        raw(
            "null_or_empty",
            vec![Arc::new(LargeStringArray::from(vec![Some(""), None]))]
        )
        .unwrap_err()
        .to_string(),
        "null_or_empty expects string or array, got LargeUtf8"
    );
}
#[test]
fn null_or_empty_unsupported_type_uses_bounded_nominal_diagnostic() {
    let field = Arc::new(Field::new("x".repeat(900), DataType::Int64, true));
    let input: ArrayRef = Arc::new(StructArray::new(
        vec![field].into(),
        vec![Arc::new(Int64Array::from(vec![Some(1)]))],
        None,
    ));
    let expected = format!(
        "null_or_empty expects string or array, got {:?}",
        input.data_type()
    );
    assert!(expected.len() > 512);
    let failure = raw("null_or_empty", vec![input]).unwrap_err();
    assert_eq!(
        failure.to_string(),
        novarocks_failure_contract::BoundedDetail::truncating_display(&expected).to_string()
    );
    assert_eq!(failure.failure_fact().class(), FailureClass::Invariant);
    assert_eq!(failure.failure_fact().reason().code(), 361);
}
#[test]
fn null_or_empty_exact_unary_shape_and_valid_child_error_are_preserved() {
    let (mut arena, args, chunk) = setup(vec![text(vec![Some("")])]);
    let missing = arena.push_typed(ExprNode::SlotId(SlotId::new(99)), DataType::Utf8);
    for arguments in [&[][..], &[args[0], missing][..]] {
        let failure = eval_string_function(
            "null_or_empty",
            &arena,
            ExprId(usize::MAX),
            arguments,
            &chunk,
        )
        .unwrap_err();
        assert_eq!(failure.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(failure.failure_fact().reason().code(), 430);
    }
    assert_eq!(
        bools(
            &eval_string_function("null_or_empty", &arena, ExprId(usize::MAX), &args, &chunk)
                .unwrap()
        ),
        vec![Some(true)]
    );
    let expected = arena.eval(missing, &chunk).unwrap_err();
    let failure = eval_string_function(
        "null_or_empty",
        &arena,
        ExprId(usize::MAX),
        &[missing],
        &chunk,
    )
    .unwrap_err();
    assert_eq!(failure.failure_fact(), expected.failure_fact());
    assert_eq!(failure.to_string(), expected.to_string());
    assert!(!failure.same_occurrence(&expected));
}
