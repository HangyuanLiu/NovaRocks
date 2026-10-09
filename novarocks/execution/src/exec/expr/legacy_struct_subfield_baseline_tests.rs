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
//! Independent original two subfield Arrow projection authors.
use super::{ExprArena, ExprId, ExprNode};
use crate::exec::chunk::{Chunk, ChunkSchema};
use arrow::array::{Array, ArrayRef, Int32Array, ListArray, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_buffer::{NullBuffer, OffsetBuffer};
use novarocks_types::SlotId;
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};
pub(super) fn structure(values: ArrayRef, valid: Option<Vec<bool>>) -> ArrayRef {
    Arc::new(StructArray::new(
        vec![Arc::new(Field::new(
            "Chosen",
            values.data_type().clone(),
            true,
        ))]
        .into(),
        vec![values],
        valid.map(NullBuffer::from),
    ))
}
pub(super) fn list(values: ArrayRef, offsets: Vec<i32>, valid: Option<Vec<bool>>) -> ArrayRef {
    Arc::new(ListArray::new(
        Arc::new(Field::new(
            "original-item",
            values.data_type().clone(),
            true,
        )),
        OffsetBuffer::new(offsets.into()),
        values,
        valid.map(NullBuffer::from),
    ))
}
fn setup(a: ArrayRef, b: ArrayRef) -> (ExprArena, [ExprId; 2], Chunk) {
    let mut arena = ExprArena::default();
    let slots = [SlotId::new(1), SlotId::new(2)];
    let ids = [
        arena.push_typed(ExprNode::SlotId(slots[0]), a.data_type().clone()),
        arena.push_typed(ExprNode::SlotId(slots[1]), b.data_type().clone()),
    ];
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("input", a.data_type().clone(), true),
            Field::new("name", b.data_type().clone(), true),
        ])),
        vec![a, b],
    )
    .unwrap();
    let cs =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
    (arena, ids, Chunk::new_with_chunk_schema(batch, cs))
}
fn raw(
    array: bool,
    a: ArrayRef,
    b: ArrayRef,
    target: Option<DataType>,
) -> Result<ArrayRef, String> {
    let (mut arena, args, chunk) = setup(a, b);
    let expr = target
        .map(|t| arena.push_typed(ExprNode::SlotId(SlotId::new(999)), t))
        .unwrap_or(ExprId(usize::MAX));
    if array {
        super::function::array::eval_array_struct_subfield(&arena, expr, &args, &chunk)
    } else {
        super::function::struct_fn::eval_subfield(&arena, expr, &args, &chunk)
    }
}
fn names(text: Option<&str>, n: usize) -> ArrayRef {
    Arc::new(StringArray::from(vec![text; n]))
}
#[test]
fn original_subfield_parent_nulls_hidden_payloads_and_child_nulls_follow_arrow_take() {
    let a = structure(
        Arc::new(Int32Array::from(vec![Some(7), Some(999), None])),
        Some(vec![true, false, true]),
    );
    let out = raw(false, a, names(Some("Chosen"), 3), None).unwrap();
    assert_eq!(
        out.to_data(),
        Int32Array::from(vec![Some(7), None, None]).to_data()
    );
}
#[test]
fn original_subfield_array_offsets_struct_nulls_top_nulls_and_slices_preserved() {
    let child = structure(
        Arc::new(Int32Array::from(vec![
            Some(8),
            Some(777),
            None,
            Some(9),
            Some(10),
        ])),
        Some(vec![true, false, true, true, true]),
    );
    let a = list(child, vec![0, 2, 3, 5], Some(vec![true, false, true]));
    for a in [a.clone(), a.slice(1, 2), a.slice(0, 0)] {
        let target = DataType::List(Arc::new(
            Field::new("projected-item", DataType::Int32, true)
                .with_metadata([("projection".into(), "authored".into())].into()),
        ));
        if a.is_empty() {
            assert_eq!(
                raw(true, a, names(Some("Chosen"), 0), Some(target)).unwrap_err(),
                "__array_struct_subfield field-name argument is empty"
            );
            continue;
        }
        let n = a.len();
        let out = raw(
            true,
            a.clone(),
            names(Some("Chosen"), n),
            Some(target.clone()),
        )
        .unwrap();
        let src = a.as_any().downcast_ref::<ListArray>().unwrap();
        let got = out.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(got.value_offsets(), src.value_offsets());
        assert_eq!(got.nulls(), src.nulls());
        assert_eq!(out.data_type(), &target);
        assert_eq!(
            got.values().to_data(),
            Int32Array::from(vec![Some(8), None, None, Some(9), Some(10)]).to_data()
        );
    }
}
#[test]
fn original_subfield_field_name_full_errors_exact_case_constant_and_type_order() {
    for array in [false, true] {
        let prefix = if array {
            "__array_struct_subfield"
        } else {
            "subfield"
        };
        let s = structure(Arc::new(Int32Array::from(vec![1, 2])), None);
        let a = if array {
            list(s, vec![0, 1, 2], None)
        } else {
            s
        };
        for (b, error) in [
            (
                names(None, 2),
                format!("{prefix} field-name argument must be non-null"),
            ),
            (
                Arc::new(StringArray::from(vec![Some("Chosen"), None])) as ArrayRef,
                format!("{prefix} field-name argument must be constant"),
            ),
            (
                Arc::new(StringArray::from(vec!["Chosen", "other"])) as ArrayRef,
                format!("{prefix} field-name argument must be constant"),
            ),
            (
                names(Some("chosen"), 2),
                format!("{prefix} field 'chosen' does not exist"),
            ),
            (
                names(Some(""), 2),
                format!("{prefix} field '' does not exist"),
            ),
            (
                Arc::new(Int32Array::from(vec![1, 1])) as ArrayRef,
                format!("{prefix} field-name argument must be VARCHAR"),
            ),
        ] {
            assert_eq!(raw(array, a.clone(), b, None).unwrap_err(), error);
        }
        let input: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        assert_eq!(
            raw(array, input.clone(), names(None, 2), None).unwrap_err(),
            format!(
                "{prefix} expects {}Array, got Int32",
                if array { "List" } else { "Struct" }
            )
        );
    }
}
#[test]
fn original_subfield_nested_children_and_duplicate_fields_use_first_exact_match() {
    let nested = list(
        Arc::new(Int32Array::from(vec![1, 2, 3])),
        vec![0, 2, 3],
        None,
    );
    let a: ArrayRef = Arc::new(StructArray::new(
        vec![
            Arc::new(Field::new("Chosen", nested.data_type().clone(), true)),
            Arc::new(Field::new("Chosen", DataType::Int32, true)),
        ]
        .into(),
        vec![nested.clone(), Arc::new(Int32Array::from(vec![90, 91]))],
        None,
    ));
    let out = raw(false, a, names(Some("Chosen"), 2), None).unwrap();
    assert_eq!(out.to_data(), nested.to_data());
    let a = list(structure(nested.clone(), None), vec![0, 1, 2], None);
    let t = DataType::List(Arc::new(Field::new(
        "item",
        nested.data_type().clone(),
        true,
    )));
    let out = raw(true, a, names(Some("Chosen"), 2), Some(t)).unwrap();
    assert_eq!(
        out.as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .values()
            .to_data(),
        nested.to_data()
    );
}
#[test]
fn original_subfield_array_untyped_target_retains_original_struct_cast_failure() {
    let a = list(
        structure(Arc::new(Int32Array::from(vec![1])), None),
        vec![0, 1],
        None,
    );
    let target = a.as_any().downcast_ref::<ListArray>().unwrap().value_type();
    let child: ArrayRef = Arc::new(Int32Array::from(vec![1]));
    let cause = arrow::compute::cast(&child, &target)
        .unwrap_err()
        .to_string();
    assert_eq!(
        raw(true, a, names(Some("Chosen"), 1), None).unwrap_err(),
        format!(
            "__array_struct_subfield: failed to cast output Int32 -> {:?}: {}",
            target, cause
        )
    );
}
#[test]
fn original_subfield_child_evaluation_order_ignored_tail_and_original_arity_panic() {
    let (arena, args, chunk) = setup(
        structure(Arc::new(Int32Array::from(vec![1])), None),
        names(Some("Chosen"), 1),
    );
    for array in [false, true] {
        let call = |xs: &[ExprId]| {
            if array {
                super::function::array::eval_array_struct_subfield(
                    &arena,
                    ExprId(usize::MAX),
                    xs,
                    &chunk,
                )
            } else {
                super::function::struct_fn::eval_subfield(&arena, ExprId(usize::MAX), xs, &chunk)
            }
        };
        assert_eq!(
            call(&[ExprId(usize::MAX), args[1]]).unwrap_err(),
            "invalid ExprId"
        );
        assert_eq!(
            call(&[args[0], ExprId(usize::MAX)]).unwrap_err(),
            "invalid ExprId"
        );
        assert!(catch_unwind(AssertUnwindSafe(|| call(&[]))).is_err());
        assert!(catch_unwind(AssertUnwindSafe(|| call(&[args[0]]))).is_err());
        if !array {
            assert!(call(&[args[0], args[1], ExprId(usize::MAX)]).is_ok());
        }
    }
}

#[test]
fn original_subfield_array_full_name_scope_and_selected_single_row_counterexample() {
    let a = list(
        structure(Arc::new(Int32Array::from(vec![1, 2, 3])), None),
        vec![0, 1, 2, 3],
        None,
    );
    let target = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
    let b: ArrayRef = Arc::new(StringArray::from(vec![
        Some("Chosen"),
        Some("other"),
        Some("Chosen"),
    ]));
    assert_eq!(
        raw(true, a.clone(), b.clone(), Some(target.clone())).unwrap_err(),
        "__array_struct_subfield field-name argument must be constant"
    );
    for row in [0, 2] {
        let output = raw(true, a.slice(row, 1), b.slice(row, 1), Some(target.clone())).unwrap();
        assert_eq!(output.len(), 1);
    }
    assert_eq!(
        raw(true, a.slice(1, 1), b.slice(1, 1), Some(target)).unwrap_err(),
        "__array_struct_subfield field 'other' does not exist"
    );
}
#[test]
fn original_subfield_array_wrongcase_on_null_and_empty_actual_invocations() {
    let target = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
    let a = list(
        structure(
            Arc::new(Int32Array::from(vec![None, None])),
            Some(vec![false, false]),
        ),
        vec![0, 1, 2],
        Some(vec![false, false]),
    );
    assert_eq!(
        raw(
            true,
            a.clone(),
            names(Some("chosen"), 2),
            Some(target.clone())
        )
        .unwrap_err(),
        "__array_struct_subfield field 'chosen' does not exist"
    );
    // An actual empty invocation evaluates an empty field-name column; this is
    // distinct from a caller that never invokes the function for an empty use.
    assert_eq!(
        raw(true, a.slice(0, 0), names(Some("Chosen"), 0), Some(target)).unwrap_err(),
        "__array_struct_subfield field-name argument is empty"
    );
}
#[test]
fn original_subfield_array_constant_expr_empty_chunk_and_null_input_do_not_skip_name() {
    use super::LiteralValue;
    let a = list(
        structure(Arc::new(Int32Array::from(vec![None])), Some(vec![false])),
        vec![0, 1],
        Some(vec![false]),
    );
    for (rows, pool) in [(0, false), (1, false), (0, true), (1, true)] {
        let mut arena = ExprArena::default();
        let slot = SlotId::new(1);
        let input = a.slice(0, rows);
        let input_id = arena.push_typed(ExprNode::SlotId(slot), input.data_type().clone());
        let name_node = if pool {
            ExprNode::Constant(super::pure_differential::constant(
                novarocks_type_contract::FunctionValueType::new(DataType::Utf8, false),
                Arc::new(StringArray::from(vec!["chosen"])),
            ))
        } else {
            ExprNode::Literal(LiteralValue::Utf8("chosen".into()))
        };
        let name_id = arena.push_typed(name_node, DataType::Utf8);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "input",
                input.data_type().clone(),
                true,
            )])),
            vec![input],
        )
        .unwrap();
        let cs = ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[slot])
            .unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, cs);
        let error = super::function::array::eval_array_struct_subfield(
            &arena,
            ExprId(usize::MAX),
            &[input_id, name_id],
            &chunk,
        )
        .unwrap_err();
        assert_eq!(
            error,
            if rows == 0 {
                "__array_struct_subfield field-name argument is empty"
            } else {
                "__array_struct_subfield field 'chosen' does not exist"
            }
        );
    }
}

#[test]
fn original_subfield_array_arena_arity_validation_precedes_all_children() {
    let (mut arena, _, chunk) = setup(
        list(
            structure(Arc::new(Int32Array::from(vec![1])), None),
            vec![0, 1],
            None,
        ),
        names(Some("Chosen"), 1),
    );
    let kind = super::function::lookup_function("__array_struct_subfield").unwrap();
    let metadata = super::function::function_metadata(kind);
    assert_eq!((metadata.min_args, metadata.max_args), (2, 2));
    for n in [0, 1, 3, 5] {
        let expr = arena.push_typed(
            ExprNode::FunctionCall {
                kind,
                args: vec![ExprId(usize::MAX); n],
            },
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        );
        assert_eq!(
            arena.eval(expr, &chunk).unwrap_err(),
            format!("{} expects 2 to 2 arguments, got {n}", metadata.name)
        );
    }
}

#[path = "legacy_array_struct_subfield_demand_tests.rs"]
mod array_demand_before;

#[path = "original_scalar_invocation_activation_baseline_tests.rs"]
mod scalar_invocation_activation_baseline;
