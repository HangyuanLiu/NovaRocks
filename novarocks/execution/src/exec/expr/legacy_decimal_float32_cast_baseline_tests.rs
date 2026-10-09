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

//! Immutable original Expr/Project/Types Decimal Float32 paths, including their deliberate projection difference.
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::{ExprArena, ExprNode, cast_array_to_target};
use arrow::array::{Array, ArrayRef, Decimal128Array, Decimal256Array, Float32Array};
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
    let cast = arena.push_typed(ExprNode::Cast(source, policy), DataType::Float32);
    arena.eval(cast, &chunk)
}
pub(super) fn project(a: &ArrayRef) -> Result<ArrayRef, String> {
    cast_array_to_target(a, &DataType::Float32)
}
pub(super) fn assert_same(a: &ArrayRef, b: &ArrayRef) {
    assert_eq!(a.data_type(), &DataType::Float32);
    assert_eq!(a.len(), b.len());
    let a = a.as_any().downcast_ref::<Float32Array>().unwrap();
    let b = b.as_any().downcast_ref::<Float32Array>().unwrap();
    for r in 0..a.len() {
        assert_eq!(a.is_null(r), b.is_null(r));
        if !a.is_null(r) {
            assert_eq!(a.value(r).to_bits(), b.value(r).to_bits(), "row {r}");
        }
    }
}

#[test]
fn legacy_decimal_float32_original_normal_rounding_and_null_all_policy_allow() {
    for wide in [false, true] {
        let a = input(
            wide,
            if wide { 76 } else { 38 },
            2,
            vec![Some(12345), Some(-1), Some(0), None],
        );
        let expected: ArrayRef = Arc::new(Float32Array::from(vec![
            Some(123.45_f32),
            Some(-0.01_f32),
            Some(0.0),
            None,
        ]));
        assert_same(&project(&a).unwrap(), &expected);
        assert_same(
            &novarocks_types::arrow_cast::cast_scalar_with_special_rules(&a, &DataType::Float32)
                .unwrap(),
            &expected,
        );
        for policy in [
            DecimalOverflowPolicy::ReportError,
            DecimalOverflowPolicy::OutputNull,
        ] {
            for allow in [false, true] {
                assert_same(&actual(&a, policy, allow).unwrap(), &expected);
            }
        }
    }
}
#[test]
fn legacy_decimal_float32_original_decimal128_expr_sanitizes_but_project_preserves_infinity() {
    let a = input(
        false,
        38,
        -1,
        vec![Some(i128::MAX), Some(i128::MIN), Some(1), Some(-1), None],
    );
    let projected: ArrayRef = Arc::new(Float32Array::from(vec![
        Some(f32::INFINITY),
        Some(f32::NEG_INFINITY),
        Some(10.0),
        Some(-10.0),
        None,
    ]));
    let expr: ArrayRef = Arc::new(Float32Array::from(vec![
        None,
        None,
        Some(10.0),
        Some(-10.0),
        None,
    ]));
    assert_same(&project(&a).unwrap(), &projected);
    assert_same(
        &novarocks_types::arrow_cast::cast_scalar_with_special_rules(&a, &DataType::Float32)
            .unwrap(),
        &projected,
    );
    for policy in [
        DecimalOverflowPolicy::ReportError,
        DecimalOverflowPolicy::OutputNull,
    ] {
        for allow in [false, true] {
            assert_same(&actual(&a, policy, allow).unwrap(), &expr);
        }
    }
    let wide: ArrayRef = Arc::new(
        Decimal256Array::from(vec![Some(i256::MAX), Some(i256::MIN), None])
            .with_precision_and_scale(76, 0)
            .unwrap(),
    );
    let ext: ArrayRef = Arc::new(Float32Array::from(vec![
        Some(f32::INFINITY),
        Some(f32::NEG_INFINITY),
        None,
    ]));
    assert_same(&project(&wide).unwrap(), &ext);
    for policy in [
        DecimalOverflowPolicy::ReportError,
        DecimalOverflowPolicy::OutputNull,
    ] {
        for allow in [false, true] {
            assert_same(&actual(&wide, policy, allow).unwrap(), &ext);
        }
    }
}
#[test]
fn legacy_decimal_float32_original_complete_p_signed_scale_paths_keep_distinct_projection() {
    for wide in [false, true] {
        let max = if wide { 76 } else { 38 };
        for p in 1..=max {
            for scale in i8::MIN..=p as i8 {
                let a = input(
                    wide,
                    p,
                    scale,
                    if wide && scale == i8::MIN {
                        vec![None]
                    } else {
                        vec![Some(i128::MIN), Some(i128::MAX), Some(1), Some(0), None]
                    },
                );
                let projected = project(&a).unwrap();
                let projected_array = projected.as_any().downcast_ref::<Float32Array>().unwrap();
                assert_same(
                    &projected,
                    &novarocks_types::arrow_cast::cast_scalar_with_special_rules(
                        &a,
                        &DataType::Float32,
                    )
                    .unwrap(),
                );
                for policy in [
                    DecimalOverflowPolicy::ReportError,
                    DecimalOverflowPolicy::OutputNull,
                ] {
                    for allow in [false, true] {
                        let expression = actual(&a, policy, allow).unwrap();
                        let expression =
                            expression.as_any().downcast_ref::<Float32Array>().unwrap();
                        for row in 0..a.len() {
                            let expected_null = projected_array.is_null(row)
                                || (!wide && !projected_array.value(row).is_finite());
                            assert_eq!(expression.is_null(row), expected_null);
                            if !expected_null {
                                assert_eq!(
                                    expression.value(row).to_bits(),
                                    projected_array.value(row).to_bits()
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}
#[test]
fn legacy_decimal_float32_original_hidden_payload_slice_empty_and_min_scale_panic() {
    let hidden: ArrayRef = Arc::new(
        Decimal128Array::new(
            vec![i128::MAX, 1].into(),
            Some(NullBuffer::from(vec![false, true])),
        )
        .with_precision_and_scale(38, -1)
        .unwrap(),
    );
    let out = project(&hidden).unwrap();
    let out = out.as_any().downcast_ref::<Float32Array>().unwrap();
    assert!(out.is_null(0));
    assert_eq!(out.value(0).to_bits(), f32::INFINITY.to_bits());
    let out = actual(&hidden, DecimalOverflowPolicy::ReportError, true).unwrap();
    let out = out.as_any().downcast_ref::<Float32Array>().unwrap();
    assert!(out.is_null(0));
    assert_eq!(out.value(0).to_bits(), 0.0_f32.to_bits());
    for wide in [false, true] {
        let a = input(
            wide,
            if wide { 76 } else { 38 },
            2,
            vec![Some(0), Some(12345), None, Some(-7)],
        );
        for a in [a.slice(1, 3), a.slice(0, 0)] {
            assert_same(
                &actual(&a, DecimalOverflowPolicy::OutputNull, false).unwrap(),
                &project(&a).unwrap(),
            );
        }
    }
    for p in [1, 76] {
        let a = input(true, p, i8::MIN, vec![Some(0)]);
        let caught =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| project(&a).unwrap()));
        assert_eq!(caught.is_err(), cfg!(debug_assertions));
        if let Ok(out) = caught {
            assert_eq!(
                out.as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap()
                    .value(0)
                    .to_bits(),
                0.0_f32.to_bits()
            );
        }
        for policy in [
            DecimalOverflowPolicy::ReportError,
            DecimalOverflowPolicy::OutputNull,
        ] {
            for allow in [false, true] {
                assert_eq!(
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| actual(
                        &a, policy, allow
                    )
                    .unwrap()))
                    .is_err(),
                    cfg!(debug_assertions)
                );
            }
        }
        let null = input(true, p, i8::MIN, vec![None]);
        assert_eq!(project(&null).unwrap().null_count(), 1);
        assert_eq!(
            actual(&null, DecimalOverflowPolicy::ReportError, true)
                .unwrap()
                .null_count(),
            1
        );
    }
}
