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
//! Original MOD/PMOD raw dispatcher oracles; no pure implementation is used.
use super::{ExprArena, ExprNode};
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::{FunctionKind, math::eval_math_function};
use arrow::array::{
    Array, ArrayRef, Decimal128Array, Decimal256Array, FixedSizeBinaryArray, Float64Array,
    Int8Array, Int64Array, NullArray, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_types::SlotId;
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};
fn evaluate(
    name: &'static str,
    columns: Vec<ArrayRef>,
    result: DataType,
) -> Result<ArrayRef, String> {
    let fields = columns
        .iter()
        .enumerate()
        .map(|(i, c)| Field::new(format!("c{i}"), c.data_type().clone(), true))
        .collect::<Vec<_>>();
    let slots = (1..=columns.len())
        .map(|i| SlotId::new(i as u32))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns.clone()).unwrap();
    let chunk_schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, chunk_schema);
    let mut arena = ExprArena::default();
    let args = columns
        .iter()
        .enumerate()
        .map(|(i, c)| arena.push_typed(ExprNode::SlotId(slots[i]), c.data_type().clone()))
        .collect::<Vec<_>>();
    let call = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Math(name),
            args: args.clone(),
        },
        result,
    );
    eval_math_function(name, &arena, call, &args, &chunk)
}
fn ints(v: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(v))
}
fn floats(out: ArrayRef) -> Vec<Option<f64>> {
    assert_eq!(out.data_type(), &DataType::Float64);
    out.as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .iter()
        .collect()
}
fn decimal(v: Vec<Option<i128>>, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(v)
            .with_precision_and_scale(38, scale)
            .unwrap(),
    )
}
#[test]
fn original_mod_pmod_full_width_signed_and_float_projection_are_frozen() {
    let left = ints(vec![
        Some(i64::MIN),
        Some(-1),
        Some(9007199254740993),
        Some(-7),
        Some(7),
        None,
    ]);
    let right = ints(vec![
        Some(-1),
        Some(i64::MIN),
        Some(2),
        Some(-3),
        Some(-3),
        Some(3),
    ]);
    assert_eq!(
        floats(evaluate("mod", vec![left.clone(), right.clone()], DataType::Float64).unwrap()),
        vec![
            Some(0.0),
            Some(-1.0),
            Some(1.0),
            Some(-1.0),
            Some(1.0),
            None
        ]
    );
    assert_eq!(
        floats(evaluate("pmod", vec![left, right], DataType::Float64).unwrap()),
        vec![
            Some(0.0),
            Some(f64::from_bits(0x43e0000000000000)),
            Some(1.0),
            Some(2.0),
            Some(1.0),
            None
        ]
    );
}
#[test]
fn original_mod_pmod_raw_decimal128_scale_and_precision_loss_are_frozen() {
    for (scale, expected_mod, expected_pmod) in
        [(2, -1.0, 2.0), (0, -1.0, 398.0), (-1, -10.0, 3980.0)]
    {
        let left = decimal(vec![Some(-799), None, Some(0)], scale);
        let right = decimal(vec![Some(399), Some(399), Some(0)], scale);
        assert_eq!(
            floats(evaluate("mod", vec![left.clone(), right.clone()], DataType::Float64).unwrap()),
            vec![Some(expected_mod), None, None]
        );
        assert_eq!(
            floats(evaluate("pmod", vec![left, right], DataType::Float64).unwrap()),
            vec![Some(expected_pmod), None, None]
        );
    }
    let wide = decimal(vec![Some(i128::MAX), Some(i128::MIN)], 0);
    let divisor = ints(vec![Some(3), Some(3)]);
    assert_eq!(
        floats(
            evaluate(
                "mod",
                vec![wide.clone(), divisor.clone()],
                DataType::Float64
            )
            .unwrap()
        ),
        vec![Some(1.0), Some(-2.0)]
    );
    assert_eq!(
        floats(evaluate("pmod", vec![wide, divisor], DataType::Float64).unwrap()),
        vec![Some(1.0), Some(1.0)]
    );
}
#[test]
fn original_mod_pmod_raw_unregistered_carriers_keep_full_admission_errors() {
    let fsb: ArrayRef =
        Arc::new(FixedSizeBinaryArray::try_from_iter(std::iter::once([0u8; 16])).unwrap());
    let d256: ArrayRef = Arc::new(
        Decimal256Array::from(vec![Some(arrow_buffer::i256::ONE)])
            .with_precision_and_scale(76, 0)
            .unwrap(),
    );
    let utf8: ArrayRef = Arc::new(StringArray::from(vec!["7"]));
    for source in [fsb, d256, utf8] {
        for name in ["mod", "pmod"] {
            let expected = format!("unsupported numeric type: {:?}", source.data_type());
            assert_eq!(
                evaluate(
                    name,
                    vec![source.clone(), ints(vec![Some(3)])],
                    DataType::Float64
                )
                .unwrap_err(),
                expected
            );
            assert_eq!(
                evaluate(
                    name,
                    vec![ints(vec![Some(7)]), source.clone()],
                    DataType::Float64
                )
                .unwrap_err(),
                expected
            );
        }
    }
}
#[test]
fn original_mod_pmod_raw_target_safe_cast_text_and_null_are_frozen() {
    for name in ["mod", "pmod"] {
        let out = evaluate(
            name,
            vec![
                ints(vec![Some(-1000), Some(7)]),
                ints(vec![Some(800), Some(3)]),
            ],
            DataType::Int8,
        )
        .unwrap();
        let v = out.as_any().downcast_ref::<Int8Array>().unwrap();
        assert_eq!(v.iter().collect::<Vec<_>>(), vec![None, Some(1)]);
        let out = evaluate(
            name,
            vec![ints(vec![Some(-7), None]), ints(vec![Some(3), Some(3)])],
            DataType::Utf8,
        )
        .unwrap();
        let v = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            v.iter().collect::<Vec<_>>(),
            vec![Some(if name == "mod" { "-1" } else { "2" }), None]
        );
        let null: ArrayRef = Arc::new(NullArray::new(2));
        assert_eq!(
            floats(
                evaluate(
                    name,
                    vec![null, ints(vec![Some(3), Some(0)])],
                    DataType::Float64
                )
                .unwrap()
            ),
            vec![None, None]
        );
    }
}
#[test]
fn original_mod_pmod_float_nonfinite_fractional_zero_and_signed_zero_are_frozen() {
    let l: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(-7.9),
        Some(f64::MAX),
        Some(-f64::MAX),
        Some(f64::NAN),
        Some(f64::INFINITY),
        Some(-0.0),
        None,
    ]));
    let r: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(-3.8),
        Some(3.0),
        Some(-1.0),
        Some(3.0),
        Some(3.0),
        Some(0.9),
        Some(3.0),
    ]));
    for name in ["mod", "pmod"] {
        assert_eq!(
            floats(evaluate(name, vec![l.clone(), r.clone()], DataType::Float64).unwrap()),
            vec![
                Some(if name == "mod" { -1.0 } else { 2.0 }),
                Some(1.0),
                Some(0.0),
                None,
                None,
                None,
                None
            ]
        );
    }
    let out = floats(
        evaluate(
            "mod",
            vec![
                Arc::new(Float64Array::from(vec![-0.0])),
                ints(vec![Some(3)]),
            ],
            DataType::Float64,
        )
        .unwrap(),
    );
    assert_eq!(out[0].unwrap().to_bits(), 0);
}
#[test]
fn original_mod_pmod_sliced_empty_and_ignored_tail_arguments_are_frozen() {
    let l = ints(vec![Some(99), Some(-7), None, Some(-8), Some(99)]).slice(1, 3);
    let r = ints(vec![Some(99), Some(3), Some(3), Some(-3), Some(99)]).slice(1, 3);
    for name in ["mod", "pmod"] {
        let expected = if name == "mod" {
            vec![Some(-1.0), None, Some(-2.0)]
        } else {
            vec![Some(2.0), None, Some(1.0)]
        };
        let ignored: ArrayRef = Arc::new(StringArray::from(vec!["ignored"; 3]));
        assert_eq!(
            floats(evaluate(name, vec![l.clone(), r.clone(), ignored], DataType::Float64).unwrap()),
            expected
        );
        let out = evaluate(name, vec![l.slice(0, 0), r.slice(0, 0)], DataType::Float64).unwrap();
        assert!(out.is_empty());
    }
}
#[test]
fn original_mod_pmod_one_argument_keeps_original_index_panic() {
    for name in ["mod", "pmod"] {
        assert!(
            catch_unwind(AssertUnwindSafe(|| evaluate(
                name,
                vec![ints(vec![Some(7)])],
                DataType::Float64
            )))
            .is_err()
        );
    }
}
