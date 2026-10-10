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

//! Original Decimal128-to-Decimal128 arena evidence. No replacement calculator.
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::{ExprArena, ExprNode};
use arrow::array::{Array, ArrayRef, Decimal128Array};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use arrow_buffer::NullBuffer;
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType};
use novarocks_types::SlotId;
use std::sync::Arc;

pub(super) const OVERFLOW: &str =
    "Expr evaluate meet error: The numeric type cast involving decimal overflows";
pub(super) fn input(p: u8, s: i8, values: Vec<Option<i128>>) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values)
            .with_precision_and_scale(p, s)
            .unwrap(),
    )
}
pub(super) fn values(array: &ArrayRef) -> Vec<Option<i128>> {
    let array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
    (0..array.len())
        .map(|r| (!array.is_null(r)).then(|| array.value(r)))
        .collect()
}
pub(super) fn actual(
    array: ArrayRef,
    target: DataType,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> Result<ArrayRef, String> {
    let ty = FunctionValueType::new(array.data_type().clone(), true);
    let data = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            ty.try_to_field("original-decimal").unwrap(),
        ])),
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
pub(super) fn modes() -> [(DecimalOverflowPolicy, bool); 4] {
    [
        (DecimalOverflowPolicy::OutputNull, false),
        (DecimalOverflowPolicy::OutputNull, true),
        (DecimalOverflowPolicy::ReportError, false),
        (DecimalOverflowPolicy::ReportError, true),
    ]
}
fn successful(array: ArrayRef, target: DataType, expected: &[Option<i128>]) {
    for (policy, allow) in modes() {
        let out = actual(array.clone(), target.clone(), policy, allow).unwrap();
        assert_eq!(out.data_type(), &target);
        assert_eq!(values(&out), expected);
    }
}
fn overflow(array: ArrayRef, target: DataType, expected: &[Option<i128>]) {
    for (policy, allow) in modes() {
        let out = actual(array.clone(), target.clone(), policy, allow);
        if policy == DecimalOverflowPolicy::OutputNull && !allow {
            let out = out.unwrap();
            assert_eq!(out.data_type(), &target);
            assert_eq!(values(&out), expected);
        } else {
            assert_eq!(out.unwrap_err(), OVERFLOW);
        }
    }
}
#[test]
fn decimal128_rescale_original_observed_sql_precision_and_scale_pairs() {
    successful(
        input(7, 2, vec![Some(12000), Some(9000), None]),
        DataType::Decimal128(9, 3),
        &[Some(120000), Some(90000), None],
    );
    successful(
        input(14, 13, vec![Some(1), Some(-2), None]),
        DataType::Decimal128(38, 13),
        &[Some(1), Some(-2), None],
    );
}
#[test]
fn decimal128_rescale_original_half_up_negative_scales_and_signs() {
    successful(
        input(
            10,
            4,
            vec![Some(3185), Some(-3185), Some(3149), Some(-3149), None],
        ),
        DataType::Decimal128(10, 2),
        &[Some(32), Some(-32), Some(31), Some(-31), None],
    );
    successful(
        input(18, -2, vec![Some(123), Some(-123), Some(0), None]),
        DataType::Decimal128(20, 0),
        &[Some(12300), Some(-12300), Some(0), None],
    );
    successful(
        input(
            18,
            2,
            vec![Some(14999), Some(15000), Some(-14999), Some(-15000), None],
        ),
        DataType::Decimal128(20, -2),
        &[Some(1), Some(2), Some(-1), Some(-2), None],
    );
}
#[test]
fn decimal128_rescale_original_upscale_multiply_and_precision_overflow_all_policies() {
    let max = 10_i128.pow(38) - 1;
    overflow(
        input(38, 0, vec![Some(max), Some(-max), Some(1), None]),
        DataType::Decimal128(38, 1),
        &[None, None, Some(10), None],
    );
    overflow(
        input(7, 2, vec![Some(99999), Some(1), None]),
        DataType::Decimal128(3, 2),
        &[None, Some(1), None],
    );
}
#[test]
fn decimal128_rescale_original_identical_metadata_still_enforces_full_carrier_precision() {
    overflow(
        input(
            1,
            0,
            vec![
                Some(i128::MIN),
                Some(i128::MAX),
                Some(9),
                Some(-9),
                Some(10),
                Some(-10),
                None,
            ],
        ),
        DataType::Decimal128(1, 0),
        &[None, None, Some(9), Some(-9), None, None, None],
    );
}
#[test]
fn decimal128_rescale_original_identical_metadata_complete_legal_signed_scales() {
    for p in 1..=38 {
        for s in i8::MIN..=p as i8 {
            overflow(
                input(
                    p,
                    s,
                    vec![Some(i128::MIN), Some(i128::MAX), Some(1), Some(-1), None],
                ),
                DataType::Decimal128(p, s),
                &[None, None, Some(1), Some(-1), None],
            );
        }
    }
}
#[test]
fn decimal128_rescale_original_scale_factor_data_error_and_null_empty_masking() {
    let target = DataType::Decimal128(38, -39);
    for (policy, allow) in modes() {
        assert_eq!(
            actual(
                input(38, 0, vec![None, Some(1)]),
                target.clone(),
                policy,
                allow
            )
            .unwrap_err(),
            "CAST failed: from Decimal128(38, 0) to Decimal128(38, -39): decimal scale overflow while casting DECIMAL"
        );
        for array in [input(38, 0, vec![None, None]), input(38, 0, vec![])] {
            let out = actual(array.clone(), target.clone(), policy, allow).unwrap();
            assert_eq!(out.data_type(), &target);
            assert_eq!(values(&out), values(&array));
        }
    }
}
#[test]
fn decimal128_rescale_original_extreme_scale_subtraction_panic_and_hidden_null() {
    // The profile uses the project's original i8 arithmetic setting. This
    // independent arithmetic witness distinguishes checked and wrapping builds.
    fn original_difference(left: i8, right: i8) -> i8 {
        left - right
    }
    let arithmetic = std::panic::catch_unwind(|| {
        original_difference(std::hint::black_box(38), std::hint::black_box(i8::MIN))
    });
    let target = DataType::Decimal128(38, i8::MIN);
    for (policy, allow) in modes() {
        let out = std::panic::catch_unwind(|| {
            actual(
                input(38, 38, vec![None, Some(1)]),
                target.clone(),
                policy,
                allow,
            )
        });
        if arithmetic.is_err() {
            assert!(
                out.is_err(),
                "the original unchecked scale difference must preserve host panic"
            );
        } else {
            assert_eq!(
                out.unwrap().unwrap_err(),
                "CAST failed: from Decimal128(38, 38) to Decimal128(38, -128): decimal scale overflow while casting DECIMAL"
            );
        }
        let hidden: ArrayRef = Arc::new(
            Decimal128Array::new(
                vec![i128::MIN, i128::MAX].into(),
                Some(NullBuffer::from(vec![false, false])),
            )
            .with_precision_and_scale(38, 38)
            .unwrap(),
        );
        assert_eq!(
            values(&actual(hidden, target.clone(), policy, allow).unwrap()),
            vec![None, None]
        );
        assert!(
            values(&actual(input(38, 38, vec![]), target.clone(), policy, allow).unwrap())
                .is_empty()
        );
    }
}
#[test]
fn decimal128_rescale_original_slice_hidden_payload_and_empty_target_metadata() {
    let array = input(7, 2, vec![Some(99999), None, Some(-123), Some(0)]);
    successful(
        array.slice(1, 3),
        DataType::Decimal128(9, 3),
        &[None, Some(-1230), Some(0)],
    );
    successful(array.slice(1, 0), DataType::Decimal128(9, 3), &[]);
    let hidden: ArrayRef = Arc::new(
        Decimal128Array::new(
            vec![i128::MIN, 7].into(),
            Some(NullBuffer::from(vec![false, true])),
        )
        .with_precision_and_scale(7, 2)
        .unwrap(),
    );
    successful(hidden, DataType::Decimal128(9, 3), &[None, Some(70)]);
}
