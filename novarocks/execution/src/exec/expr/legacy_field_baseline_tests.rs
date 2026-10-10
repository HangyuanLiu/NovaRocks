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
//! Independent original FIELD arity, comparison, sequencing and full diagnostics.
use super::{ExprArena, ExprId, ExprNode};
use crate::exec::chunk::{Chunk, ChunkSchema};
use arrow::array::{
    Array, ArrayRef, Float32Array, Float64Array, Int32Array, NullArray, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
use novarocks_types::SlotId;
use std::sync::Arc;

pub(super) fn setup(inputs: &[ArrayRef]) -> (ExprArena, Vec<ExprId>, Chunk) {
    let mut arena = ExprArena::default();
    let slots: Vec<_> = (0..inputs.len())
        .map(|i| SlotId::new(i as u32 + 1))
        .collect();
    let args = inputs
        .iter()
        .zip(&slots)
        .map(|(a, slot)| arena.push_typed(ExprNode::SlotId(*slot), a.data_type().clone()))
        .collect();
    let fields = inputs
        .iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("original-{i}"), a.data_type().clone(), true))
        .collect::<Vec<_>>();
    let batch = if inputs.is_empty() {
        RecordBatch::new_empty(Arc::new(Schema::empty()))
    } else {
        RecordBatch::try_new(Arc::new(Schema::new(fields)), inputs.to_vec()).unwrap()
    };
    let cs =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
    (arena, args, Chunk::new_with_chunk_schema(batch, cs))
}
pub(super) fn call(arena: &ExprArena, args: &[ExprId], chunk: &Chunk) -> Result<ArrayRef, String> {
    super::function::string::eval_string_function("field", arena, ExprId(usize::MAX), args, chunk)
}
pub(super) fn raw(inputs: &[ArrayRef]) -> Result<ArrayRef, String> {
    let (arena, args, chunk) = setup(inputs);
    call(&arena, &args, &chunk)
}
fn indices(a: ArrayRef) -> Vec<i32> {
    assert_eq!(a.data_type(), &DataType::Int32);
    assert_eq!(a.null_count(), 0);
    a.as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .values()
        .to_vec()
}
pub(super) fn profiles() -> Vec<FunctionValueType> {
    let mut out = vec![
        DataType::Null,
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Decimal128(38, -2),
        DataType::Decimal128(38, 2),
        DataType::Decimal256(76, -2),
        DataType::Decimal256(76, 2),
        DataType::FixedSizeBinary(16),
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Date32,
    ]
    .into_iter()
    .map(|ty| FunctionValueType::new(ty, true))
    .collect::<Vec<_>>();
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        for zone in [
            None,
            Some("UTC".into()),
            Some("invalid-only-equality".into()),
        ] {
            out.push(FunctionValueType::new(
                DataType::Timestamp(unit.clone(), zone),
                true,
            ));
        }
    }
    out.push(
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::LargeInt,
        )
        .unwrap(),
    );
    out
}
pub(super) fn generated(ty: &FunctionValueType, n: usize) -> ArrayRef {
    use super::pure_differential::generate::{InputGenerator, InputProfile};
    // Physical FixedSizeBinary(16) has the same concrete carrier as the admitted
    // LARGEINT author. Equality does not reinterpret or normalize its bytes.
    let generated_ty = if ty.data_type == DataType::FixedSizeBinary(16) {
        FunctionValueType::try_with_logical_type(
            ty.data_type.clone(),
            ty.nullable,
            ValueLogicalType::LargeInt,
        )
        .unwrap()
    } else {
        ty.clone()
    };
    InputGenerator::new(9932).column(&generated_ty, n, &InputProfile::default())
}
#[test]
fn field_original_text_first_match_unicode_nul_null_slices_empty() {
    let a: ArrayRef = Arc::new(StringArray::from(vec![
        Some("b"),
        None,
        Some("九\0"),
        Some("missing"),
    ]));
    let b: ArrayRef = Arc::new(StringArray::from(vec![
        Some("a"),
        None,
        Some("九\0"),
        Some("x"),
    ]));
    let c: ArrayRef = Arc::new(StringArray::from(vec![
        Some("b"),
        Some("b"),
        Some("九\0"),
        Some("y"),
    ]));
    assert_eq!(
        indices(raw(&[a.clone(), b.clone(), c.clone()]).unwrap()),
        vec![2, 0, 1, 0]
    );
    assert_eq!(
        indices(raw(&[a.slice(1, 2), b.slice(1, 2), c.slice(1, 2)]).unwrap()),
        vec![0, 1]
    );
    assert!(indices(raw(&[a.slice(0, 0), b.slice(0, 0)]).unwrap()).is_empty());
}
#[test]
fn field_original_float_ieee_zero_nan_inf_first_match() {
    let left = vec![
        Some(0.),
        Some(-0.),
        Some(f64::NAN),
        Some(f64::INFINITY),
        None,
        Some(3.),
    ];
    let right = vec![
        Some(-0.),
        Some(0.),
        Some(f64::NAN),
        Some(f64::INFINITY),
        Some(3.),
        Some(4.),
    ];
    let tail = vec![Some(0.), Some(1.), Some(0.), Some(0.), None, Some(3.)];
    for width in [32, 64] {
        let arrays: Vec<ArrayRef> = [left.clone(), right.clone(), tail.clone()]
            .into_iter()
            .map(|v| {
                if width == 32 {
                    Arc::new(Float32Array::from(
                        v.into_iter()
                            .map(|v| v.map(|v| v as f32))
                            .collect::<Vec<_>>(),
                    )) as ArrayRef
                } else {
                    Arc::new(Float64Array::from(v)) as ArrayRef
                }
            })
            .collect();
        assert_eq!(indices(raw(&arrays).unwrap()), vec![1, 1, 0, 1, 0, 2]);
    }
}
#[test]
fn field_original_every_registered_flat_carrier_equal_self_preserves_meta() {
    for ty in profiles() {
        let a = generated(&ty, 257);
        for a in [a.clone(), a.slice(1, 7), a.slice(0, 0)] {
            let out = indices(raw(&[a.clone(), a.clone(), a.clone()]).unwrap());
            let expected = (0..a.len())
                .map(|r| {
                    if ty.data_type == DataType::Null || a.is_null(r) {
                        0
                    } else if let Some(f) = a.as_any().downcast_ref::<Float32Array>() {
                        i32::from(!f.value(r).is_nan())
                    } else if let Some(f) = a.as_any().downcast_ref::<Float64Array>() {
                        i32::from(!f.value(r).is_nan())
                    } else {
                        1
                    }
                })
                .collect::<Vec<_>>();
            assert_eq!(out, expected, "original FIELD carrier {ty:?}");
        }
    }
}
#[test]
fn field_original_null_first_still_demands_candidates_but_ignores_their_types() {
    let null: ArrayRef = Arc::new(NullArray::new(3));
    let text: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
    let n: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    assert_eq!(
        indices(raw(&[null.clone(), text, n]).unwrap()),
        vec![0, 0, 0]
    );
    let (arena, mut args, chunk) = setup(&[null]);
    args.push(ExprId(usize::MAX));
    assert_eq!(call(&arena, &args, &chunk).unwrap_err(), "invalid ExprId");
}
#[test]
fn field_original_arity_before_children_and_match_does_not_skip_tail() {
    let (arena, _, chunk) = setup(&[]);
    for args in [vec![], vec![ExprId(usize::MAX)]] {
        assert_eq!(
            call(&arena, &args, &chunk).unwrap_err(),
            "field requires a value and an INT-bounded candidate list"
        );
    }
    let source: ArrayRef = Arc::new(Int32Array::from(vec![7]));
    let (arena, mut args, chunk) = setup(&[source.clone(), source]);
    args.push(ExprId(usize::MAX));
    assert_eq!(call(&arena, &args, &chunk).unwrap_err(), "invalid ExprId");
}
#[test]
fn field_original_complete_mismatch_data_precedes_later_child_and_is_not_truncated() {
    let zone = "unknown-long-zone".repeat(80);
    let a: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![7]).with_timezone(zone));
    let b: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![7]));
    let expected = format!(
        "field frozen argument mismatch: {:?}/1 vs {:?}/1",
        a.data_type(),
        b.data_type()
    );
    assert!(expected.len() > 512);
    let (arena, mut args, chunk) = setup(&[a, b]);
    args.push(ExprId(usize::MAX));
    assert_eq!(call(&arena, &args, &chunk).unwrap_err(), expected);
}
#[test]
fn field_original_pool_nonzero_ordinal_broadcast_preserves_first_candidate() {
    use super::pure_differential::constant_policy;
    struct Control;
    impl novarocks_type_contract::PureCompileControl for Control {
        fn checkpoint(
            &self,
            _: novarocks_type_contract::CompilePhase,
            _: u32,
        ) -> Result<(), novarocks_type_contract::CompileControlError> {
            Ok(())
        }
    }
    use novarocks_functions::ConstantPool;
    use novarocks_type_contract::CompilePhase;
    let backing: ArrayRef = Arc::new(StringArray::from(vec![Some("ignored"), Some("b"), None]));
    let ty = FunctionValueType::new(DataType::Utf8, true);
    let pool = ConstantPool::try_new(
        Arc::new(ty.try_to_field("actual-pool").unwrap()),
        ty,
        backing.to_data(),
        constant_policy(),
        CompilePhase::FunctionSpecialization,
        &Control,
    )
    .unwrap();
    let candidate: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "b", "a"]));
    let (mut arena, args, chunk) = setup(&[candidate]);
    let first = arena.push_typed(ExprNode::Constant(pool.value(1).unwrap()), DataType::Utf8);
    assert_eq!(
        indices(call(&arena, &[first, args[0]], &chunk).unwrap()),
        vec![0, 1, 1, 0]
    );
}
