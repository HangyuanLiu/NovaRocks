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

//! Immutable original ExprArena and Project-special-cast Decimal-to-Float64 evidence.
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::{ExprArena, ExprNode, cast_array_to_target};
use arrow::array::{Array, ArrayRef, Decimal128Array, Decimal256Array, Float64Array};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use arrow_buffer::{NullBuffer, i256};
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType};
use novarocks_types::SlotId;
use std::sync::Arc;
pub(super) fn input(wide: bool, p: u8, s: i8, values: Vec<Option<i128>>) -> ArrayRef {
    if wide {
        Arc::new(
            Decimal256Array::from(
                values
                    .into_iter()
                    .map(|v| v.map(i256::from_i128))
                    .collect::<Vec<_>>(),
            )
            .with_precision_and_scale(p, s)
            .unwrap(),
        )
    } else {
        Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(p, s)
                .unwrap(),
        )
    }
}
pub(super) fn actual(
    a: &ArrayRef,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> Result<ArrayRef, String> {
    let ty = FunctionValueType::new(a.data_type().clone(), true);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            ty.try_to_field("original-decimal").unwrap(),
        ])),
        vec![a.clone()],
    )
    .unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[SlotId::new(1)])
            .unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, schema);
    let mut arena = ExprArena::default();
    arena.set_allow_throw_exception(allow);
    let source = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), a.data_type().clone());
    let cast = arena.push_typed(ExprNode::Cast(source, policy), DataType::Float64);
    arena.eval(cast, &chunk)
}
pub(super) fn project(a: &ArrayRef) -> Result<ArrayRef, String> {
    cast_array_to_target(a, &DataType::Float64)
}
pub(super) fn assert_same(a: &ArrayRef, b: &ArrayRef) {
    assert_eq!(a.data_type(), &DataType::Float64);
    assert_eq!(a.len(), b.len());
    let a = a.as_any().downcast_ref::<Float64Array>().unwrap();
    let b = b.as_any().downcast_ref::<Float64Array>().unwrap();
    for r in 0..a.len() {
        assert_eq!(a.is_null(r), b.is_null(r));
        if !a.is_null(r) {
            assert_eq!(a.value(r).to_bits(), b.value(r).to_bits(), "row {r}");
        }
    }
}
#[test]
fn legacy_decimal_float_original_expr_project_and_types_all_policies_normal_bytes() {
    for wide in [false, true] {
        let a = input(
            wide,
            if wide { 76 } else { 38 },
            2,
            vec![Some(12345), Some(-1), Some(0), None],
        );
        let expected: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(123.45),
            Some(-0.01),
            Some(0.0),
            None,
        ]));
        assert_same(&project(&a).unwrap(), &expected);
        assert_same(
            &novarocks_types::arrow_cast::cast_scalar_with_special_rules(&a, &DataType::Float64)
                .unwrap(),
            &expected,
        );
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for allow in [false, true] {
                assert_same(&actual(&a, policy, allow).unwrap(), &expected);
            }
        }
    }
}
#[test]
fn legacy_decimal_float_original_complete_legal_type_domain_expr_matches_project() {
    for wide in [false, true] {
        let max = if wide { 76 } else { 38 };
        for p in 1..=max {
            for s in i8::MIN..=p as i8 {
                // Full signed-scale admission is retained; the original -128 i8 panic has its own immutable raw oracle below.
                let a = input(
                    wide,
                    p,
                    s,
                    if wide && s == i8::MIN {
                        vec![None]
                    } else {
                        vec![Some(i128::MIN), Some(i128::MAX), Some(1), Some(-1), None]
                    },
                );
                let expected = project(&a).unwrap();
                for policy in [
                    DecimalOverflowPolicy::OutputNull,
                    DecimalOverflowPolicy::ReportError,
                ] {
                    for allow in [false, true] {
                        assert_same(&actual(&a, policy, allow).unwrap(), &expected);
                    }
                }
            }
        }
    }
}
#[test]
fn legacy_decimal_float_original_empty_slice_hidden_null_and_full_256_carrier() {
    for wide in [false, true] {
        let a = input(
            wide,
            if wide { 76 } else { 38 },
            -3,
            vec![Some(0), None, Some(-7), Some(100)],
        );
        for a in [a.slice(1, 2), a.slice(0, 0)] {
            assert_same(
                &actual(&a, DecimalOverflowPolicy::ReportError, true).unwrap(),
                &project(&a).unwrap(),
            );
        }
    }
    let hidden128: ArrayRef = Arc::new(
        Decimal128Array::new(
            vec![i128::MIN, 1].into(),
            Some(NullBuffer::from(vec![false, true])),
        )
        .with_precision_and_scale(38, 0)
        .unwrap(),
    );
    let original = project(&hidden128).unwrap();
    let original = original.as_any().downcast_ref::<Float64Array>().unwrap();
    assert!(original.is_null(0));
    assert_eq!(
        original.value(0).to_bits(),
        (i128::MIN as f64).to_bits(),
        "original Arrow unary computes hidden NULL backing payload"
    );
    let wide: ArrayRef = Arc::new(
        Decimal256Array::new(
            vec![i256::MIN, i256::MAX, i256::from_i128(1)].into(),
            Some(NullBuffer::from(vec![false, false, true])),
        )
        .with_precision_and_scale(76, 0)
        .unwrap(),
    );
    let expected: ArrayRef = Arc::new(Float64Array::from(vec![None, None, Some(1.0)]));
    assert_same(
        &actual(&wide, DecimalOverflowPolicy::OutputNull, false).unwrap(),
        &expected,
    );
    let hidden: ArrayRef = Arc::new(
        Decimal256Array::new(
            vec![i256::MIN, i256::MAX].into(),
            Some(NullBuffer::from(vec![false, false])),
        )
        .with_precision_and_scale(76, i8::MIN)
        .unwrap(),
    );
    assert_eq!(project(&hidden).unwrap().null_count(), 2);
    assert_eq!(
        actual(&hidden, DecimalOverflowPolicy::ReportError, true)
            .unwrap()
            .null_count(),
        2
    );
    let ext: ArrayRef = Arc::new(
        Decimal256Array::from(vec![Some(i256::MIN), Some(i256::MAX)])
            .with_precision_and_scale(76, 0)
            .unwrap(),
    );
    let expected: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(-5.78960446186581e76),
        Some(5.78960446186581e76),
    ]));
    assert_same(&project(&ext).unwrap(), &expected);
}
#[test]
fn legacy_decimal_float_original_min_scale_debug_panic_or_release_wrapping_not_fixed() {
    for p in [1, 76] {
        let a = input(true, p, i8::MIN, vec![Some(1)]);
        let old = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| project(&a).unwrap()));
        if cfg!(debug_assertions) {
            assert!(
                old.is_err(),
                "original -scale i8 debug overflow must remain"
            );
        } else {
            let result = old.unwrap();
            assert_eq!(
                result
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(0)
                    .to_bits(),
                (10f64.powi(-128)).to_bits()
            );
        }
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for allow in [false, true] {
                let old = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    actual(&a, policy, allow).unwrap()
                }));
                assert_eq!(old.is_err(), cfg!(debug_assertions));
            }
        }
    }
    let a = input(false, 38, i8::MIN, vec![Some(1)]);
    let old = project(&a).unwrap();
    assert_eq!(
        old.as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
            .to_bits(),
        (1f64 / 10f64.powi(-128)).to_bits()
    );
}
