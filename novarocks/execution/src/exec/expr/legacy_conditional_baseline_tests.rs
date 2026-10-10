// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Immutable v1 conditional oracles before extracting value calculations.
//! The legacy shells intentionally retain their existing argument scheduling,
//! casts, inference, Arrow errors, and carrier quirks.
use super::{ExprArena, ExprId, ExprNode, LiteralValue};
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::{self, FunctionKind};
use arrow::array::builder::{Int32Builder, ListBuilder, MapBuilder, StringBuilder};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, LargeListArray, ListArray, NullArray,
    StringArray, StructArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, new_null_array,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field, Int32Type, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use novarocks_types::SlotId;
use std::sync::Arc;

fn chunk(columns: Vec<ArrayRef>) -> Chunk {
    let fields = columns
        .iter()
        .enumerate()
        .map(|(i, c)| Field::new(format!("c{}", i + 1), c.data_type().clone(), true))
        .collect::<Vec<_>>();
    let slots = (1..=columns.len())
        .map(|i| SlotId::new(i as u32))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
    Chunk::new_with_chunk_schema(batch, schema)
}
fn invocation(
    name: &str,
    columns: &[ArrayRef],
    output: DataType,
) -> (ExprArena, ExprId, Vec<ExprId>) {
    let mut arena = ExprArena::default();
    let args = columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            arena.push_typed(
                ExprNode::SlotId(SlotId::new(i as u32 + 1)),
                c.data_type().clone(),
            )
        })
        .collect::<Vec<_>>();
    let kind = match name {
        "ifnull" => FunctionKind::IfNull,
        "coalesce" => FunctionKind::Coalesce,
        "nullif" => FunctionKind::NullIf,
        _ => unreachable!(),
    };
    let root = arena.push_typed(
        ExprNode::FunctionCall {
            kind,
            args: args.clone(),
        },
        output,
    );
    (arena, root, args)
}
fn call(
    name: &str,
    arena: &ExprArena,
    root: ExprId,
    args: &[ExprId],
    input: &Chunk,
) -> Result<ArrayRef, String> {
    match name {
        "ifnull" => function::eval_ifnull(arena, args[0], args[1], input),
        "coalesce" => function::eval_coalesce(arena, root, args, input),
        "nullif" => function::eval_nullif(arena, root, args[0], args[1], input),
        _ => unreachable!(),
    }
}
fn eval(name: &str, columns: Vec<ArrayRef>, output: DataType) -> Result<ArrayRef, String> {
    let (arena, root, args) = invocation(name, &columns, output);
    call(name, &arena, root, &args, &chunk(columns))
}
fn assert_array(output: ArrayRef, expected: ArrayRef) {
    assert_eq!(output.data_type(), expected.data_type());
    assert_eq!(output.to_data(), expected.to_data());
}

#[test]
fn legacy_conditional_baseline_nullif_every_scalar_carrier() {
    macro_rules! check {
        ($array:ty, $a:expr, $b:expr, $c:expr) => {{
            let left: ArrayRef = Arc::new(<$array>::from(vec![Some($a), Some($b), None, Some($c)]));
            let right: ArrayRef =
                Arc::new(<$array>::from(vec![Some($a), Some($c), Some($b), None]));
            let expected: ArrayRef = Arc::new(<$array>::from(vec![None, Some($b), None, Some($c)]));
            assert_array(
                eval(
                    "nullif",
                    vec![left.clone(), right],
                    left.data_type().clone(),
                )
                .unwrap(),
                expected,
            );
        }};
    }
    check!(Int8Array, 1, 2, 3);
    check!(Int16Array, 1, 2, 3);
    check!(Int32Array, 1, 2, 3);
    check!(Int64Array, 1, 2, 3);
    check!(Float32Array, 1.0, 2.0, 3.0);
    check!(Float64Array, 1.0, 2.0, 3.0);
    check!(Date32Array, 1, 2, 3);
    check!(BooleanArray, false, true, false);
    check!(StringArray, "", "中", "other");
}

#[test]
fn legacy_conditional_baseline_nullif_float_nan_and_signed_zero() {
    for wide in [false, true] {
        let (left, right): (ArrayRef, ArrayRef) = if wide {
            (
                Arc::new(Float64Array::from(vec![
                    f64::NAN,
                    -0.0,
                    f64::INFINITY,
                    f64::NEG_INFINITY,
                ])),
                Arc::new(Float64Array::from(vec![
                    f64::NAN,
                    0.0,
                    f64::INFINITY,
                    f64::INFINITY,
                ])),
            )
        } else {
            (
                Arc::new(Float32Array::from(vec![
                    f32::NAN,
                    -0.0,
                    f32::INFINITY,
                    f32::NEG_INFINITY,
                ])),
                Arc::new(Float32Array::from(vec![
                    f32::NAN,
                    0.0,
                    f32::INFINITY,
                    f32::INFINITY,
                ])),
            )
        };
        let output = eval(
            "nullif",
            vec![left.clone(), right],
            left.data_type().clone(),
        )
        .unwrap();
        assert_eq!(
            (0..4).map(|i| output.is_null(i)).collect::<Vec<_>>(),
            vec![false, true, true, false]
        );
        if wide {
            let values = output.as_any().downcast_ref::<Float64Array>().unwrap();
            assert!(values.value(0).is_nan());
            assert_eq!(values.value(0).to_bits(), f64::NAN.to_bits());
            assert_eq!(values.value(3), f64::NEG_INFINITY);
        } else {
            let values = output.as_any().downcast_ref::<Float32Array>().unwrap();
            assert!(values.value(0).is_nan());
            assert_eq!(values.value(0).to_bits(), f32::NAN.to_bits());
            assert_eq!(values.value(3), f32::NEG_INFINITY);
        }
    }
}

#[test]
fn legacy_conditional_baseline_nullif_timestamp_four_units_loses_timezone() {
    macro_rules! check {
        ($array:ty, $unit:expr) => {{
            for timezone in [None, Some("UTC"), Some("Asia/Shanghai")] {
                let left: ArrayRef = Arc::new(
                    <$array>::from(vec![Some(1), Some(2), None, Some(3)])
                        .with_timezone_opt(timezone),
                );
                let right: ArrayRef = Arc::new(
                    <$array>::from(vec![Some(1), Some(9), Some(2), None])
                        .with_timezone_opt(timezone),
                );
                let actual = eval(
                    "nullif",
                    vec![left, right],
                    DataType::Timestamp($unit, timezone.map(Into::into)),
                )
                .unwrap();
                let expected: ArrayRef =
                    Arc::new(<$array>::from(vec![None, Some(2), None, Some(3)]));
                assert_array(actual, expected);
            }
        }};
    }
    check!(TimestampSecondArray, TimeUnit::Second);
    check!(TimestampMillisecondArray, TimeUnit::Millisecond);
    check!(TimestampMicrosecondArray, TimeUnit::Microsecond);
    check!(TimestampNanosecondArray, TimeUnit::Nanosecond);
}

#[test]
fn legacy_conditional_baseline_nullif_decimal_metadata_and_casts() {
    for (precision, scale) in [(9, 0), (18, 2), (38, -3)] {
        let make = |values| {
            Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(precision, scale)
                    .unwrap(),
            ) as ArrayRef
        };
        assert_array(
            eval(
                "nullif",
                vec![
                    make(vec![Some(1), Some(2), None, Some(3)]),
                    make(vec![Some(1), Some(9), Some(2), None]),
                ],
                DataType::Decimal128(precision, scale),
            )
            .unwrap(),
            make(vec![None, Some(2), None, Some(3)]),
        );
    }
    let left: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(100), Some(201)])
            .with_precision_and_scale(9, 2)
            .unwrap(),
    );
    let right: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(10), Some(20)])
            .with_precision_and_scale(9, 1)
            .unwrap(),
    );
    let expected: ArrayRef = Arc::new(
        Decimal128Array::from(vec![None, Some(201)])
            .with_precision_and_scale(18, 2)
            .unwrap(),
    );
    assert_array(
        eval("nullif", vec![left, right], DataType::Decimal128(18, 2)).unwrap(),
        expected,
    );
}

fn lists(large: bool, right: bool) -> ArrayRef {
    let values = if right {
        vec![
            Some(vec![Some(1)]),
            Some(vec![Some(9)]),
            Some(vec![Some(4)]),
            None,
        ]
    } else {
        vec![
            Some(vec![Some(1)]),
            Some(vec![Some(2)]),
            None,
            Some(vec![Some(3)]),
        ]
    };
    if large {
        Arc::new(LargeListArray::from_iter_primitive::<Int32Type, _, _>(
            values,
        ))
    } else {
        Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(values))
    }
}
fn maps(right: bool) -> ArrayRef {
    let mut builder = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
    for value in if right {
        [Some(1), Some(9), Some(4), None]
    } else {
        [Some(1), Some(2), None, Some(3)]
    } {
        if let Some(value) = value {
            builder.keys().append_value("a");
            builder.values().append_value(value);
            builder.append(true).unwrap();
        } else {
            builder.append(false).unwrap();
        }
    }
    Arc::new(builder.finish())
}
fn structs(right: bool) -> ArrayRef {
    let values = if right {
        vec![1, 9, 4, 0]
    } else {
        vec![1, 2, 0, 3]
    };
    let valid = if right {
        vec![true, true, true, false]
    } else {
        vec![true, true, false, true]
    };
    Arc::new(StructArray::new(
        vec![Arc::new(Field::new("value", DataType::Int32, false))].into(),
        vec![Arc::new(Int32Array::from(values))],
        Some(NullBuffer::from(valid)),
    ))
}
#[test]
fn legacy_conditional_baseline_nullif_complex_display_equality_and_slices() {
    for (left, right) in [
        (lists(false, false), lists(false, true)),
        (lists(true, false), lists(true, true)),
        (maps(false), maps(true)),
        (structs(false), structs(true)),
    ] {
        for (offset, len) in [(0, 4), (1, 3)] {
            let left = left.slice(offset, len);
            let actual = eval(
                "nullif",
                vec![left.clone(), right.slice(offset, len)],
                left.data_type().clone(),
            )
            .unwrap();
            assert_eq!(actual.data_type(), left.data_type());
            let expected = [true, false, true, false];
            assert_eq!(
                (0..len).map(|i| actual.is_null(i)).collect::<Vec<_>>(),
                expected[offset..offset + len]
            );
            // Keep the original complex carrier's visible display values at retained rows.
            let opts = arrow::util::display::FormatOptions::default().with_null("\\N");
            let before =
                arrow::util::display::ArrayFormatter::try_new(left.as_ref(), &opts).unwrap();
            let after =
                arrow::util::display::ArrayFormatter::try_new(actual.as_ref(), &opts).unwrap();
            for i in 0..len {
                if !actual.is_null(i) {
                    assert_eq!(after.value(i).to_string(), before.value(i).to_string());
                }
            }
        }
    }
}

#[test]
fn legacy_conditional_baseline_nullif_unsupported_and_full_long_error() {
    let types = vec![
        DataType::Null,
        DataType::Binary,
        DataType::LargeUtf8,
        DataType::FixedSizeList(
            Arc::new(Field::new("x".repeat(900), DataType::Int32, true)),
            1,
        ),
    ];
    for ty in types {
        let input = new_null_array(&ty, 2);
        let expected = format!("nullif unsupported type: {ty:?}");
        let actual = eval("nullif", vec![input.clone(), input], ty).unwrap_err();
        assert_eq!(actual.as_bytes(), expected.as_bytes());
        if expected.contains(&"x".repeat(900)) {
            assert!(actual.len() > 512);
        }
    }
}

#[test]
fn legacy_conditional_baseline_ifnull_left_type_ignores_declared_result() {
    let left: ArrayRef = Arc::new(Int8Array::from(vec![Some(7), None, None]));
    let right: ArrayRef = Arc::new(Int64Array::from(vec![Some(1000), Some(120), Some(1000)]));
    assert_array(
        eval("ifnull", vec![left, right], DataType::Int64).unwrap(),
        Arc::new(Int8Array::from(vec![Some(7), Some(120), None])),
    );
    let left: ArrayRef = Arc::new(NullArray::new(2));
    let right: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None]));
    assert_array(
        eval("ifnull", vec![left, right.clone()], DataType::Int64).unwrap(),
        right,
    );
}

#[test]
fn legacy_conditional_baseline_coalesce_recursive_null_head_reinfers_type() {
    let left: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>, None]));
    let right: ArrayRef = Arc::new(Int64Array::from(vec![Some(9), None]));
    // Initial target is Utf8. The original all-NULL-head recursion evaluates
    // the tail again and re-infers Int64, rather than returning the first cast.
    assert_array(
        eval("coalesce", vec![left, right.clone()], DataType::Null).unwrap(),
        right,
    );
}

#[test]
fn legacy_conditional_baseline_all_null_carrier_results_differ() {
    let input: ArrayRef = Arc::new(NullArray::new(3));
    assert_array(
        eval("ifnull", vec![input.clone(), input.clone()], DataType::Null).unwrap(),
        input.clone(),
    );
    assert_array(
        eval(
            "coalesce",
            vec![input.clone(), input.clone()],
            DataType::Null,
        )
        .unwrap(),
        Arc::new(Int64Array::from(vec![None::<i64>; 3])),
    );
    assert_eq!(
        eval("nullif", vec![input.clone(), input], DataType::Null).unwrap_err(),
        "nullif unsupported type: Null"
    );
}

#[test]
fn legacy_conditional_baseline_eager_hidden_child_errors_and_left_first() {
    let input = chunk(vec![Arc::new(Int64Array::from(vec![1, 2]))]);
    for name in ["ifnull", "coalesce", "nullif"] {
        let mut arena = ExprArena::default();
        let first = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
        let hidden = arena.push_typed(ExprNode::SlotId(SlotId::new(99)), DataType::Int64);
        let root = arena.push_typed(ExprNode::Literal(LiteralValue::Null), DataType::Int64);
        let expected = arena.eval(hidden, &input).unwrap_err();
        assert_eq!(
            call(name, &arena, root, &[first, hidden], &input).unwrap_err(),
            expected
        );
        let first_failure = arena.push_typed(ExprNode::SlotId(SlotId::new(98)), DataType::Int64);
        assert_eq!(
            call(name, &arena, root, &[first_failure, hidden], &input).unwrap_err(),
            arena.eval(first_failure, &input).unwrap_err()
        );
    }
}

#[test]
fn legacy_conditional_baseline_raw_cast_errors_eager_and_full() {
    let bad: ArrayRef = Arc::new(StructArray::new(
        vec![Arc::new(Field::new(
            "bad".repeat(300),
            DataType::Int64,
            false,
        ))]
        .into(),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
        None,
    ));
    let good: ArrayRef = Arc::new(Int64Array::from(vec![7, 8]));
    let raw = arrow::compute::cast(bad.as_ref(), &DataType::Int64)
        .unwrap_err()
        .to_string();
    assert!(raw.len() > 512);
    assert_eq!(
        eval("ifnull", vec![good.clone(), bad.clone()], DataType::Int64).unwrap_err(),
        raw
    );
    assert_eq!(
        eval("coalesce", vec![good.clone(), bad.clone()], DataType::Int64).unwrap_err(),
        format!(
            "coalesce: failed to cast array from {:?} to {:?}: {}",
            bad.data_type(),
            DataType::Int64,
            raw
        )
    );
    assert_eq!(
        eval("nullif", vec![good.clone(), bad.clone()], DataType::Int64).unwrap_err(),
        format!(
            "nullif failed to cast right {:?} -> {:?}: {}",
            bad.data_type(),
            DataType::Int64,
            raw
        )
    );
    assert_eq!(
        eval("nullif", vec![bad.clone(), good], DataType::Int64).unwrap_err(),
        format!(
            "nullif failed to cast left {:?} -> {:?}: {}",
            bad.data_type(),
            DataType::Int64,
            raw
        )
    );
}

#[test]
fn legacy_conditional_baseline_coalesce_raw_arity_and_missing_output() {
    let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![Some(1), None]))];
    let (arena, root, args) = invocation("coalesce", &columns, DataType::Int64);
    let input = chunk(columns.clone());
    assert_eq!(
        call("coalesce", &arena, root, &[], &input).unwrap_err(),
        "coalesce: requires at least one argument"
    );
    assert_array(
        call("coalesce", &arena, root, &args, &input).unwrap(),
        columns[0].clone(),
    );
    assert_eq!(
        arena.eval(root, &input).unwrap_err(),
        format!("coalesce expects 2 to {} arguments, got 1", usize::MAX)
    );
    assert_eq!(
        call("coalesce", &arena, ExprId(usize::MAX), &args, &input).unwrap_err(),
        "coalesce: missing output type"
    );
}

#[test]
fn legacy_conditional_baseline_coalesce_and_ifnull_slices_empty_and_width_cast() {
    let left: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(99),
        None,
        Some(2),
        None,
        Some(88),
    ]));
    let right: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(90),
        Some(10),
        Some(20),
        None,
        Some(80),
    ]));
    let last: ArrayRef = Arc::new(Int64Array::from(vec![Some(1); 5]));
    assert_array(
        eval(
            "coalesce",
            vec![left.slice(1, 3), right.slice(1, 3), last.slice(1, 3)],
            DataType::Int64,
        )
        .unwrap(),
        Arc::new(Int64Array::from(vec![10, 2, 1])),
    );
    assert_array(
        eval(
            "ifnull",
            vec![left.slice(1, 3), right.slice(1, 3)],
            DataType::Int64,
        )
        .unwrap(),
        Arc::new(Int32Array::from(vec![Some(10), Some(2), None])),
    );
    for name in ["coalesce", "ifnull", "nullif"] {
        let output = eval(
            name,
            vec![left.slice(2, 0), right.slice(2, 0)],
            DataType::Int64,
        )
        .unwrap();
        assert_eq!(output.len(), 0);
        assert_eq!(
            output.data_type(),
            &if name == "ifnull" {
                DataType::Int32
            } else {
                DataType::Int64
            }
        );
    }
}

#[test]
fn legacy_conditional_baseline_nullif_preserves_list_display_collisions() {
    let mut left = ListBuilder::new(StringBuilder::new());
    left.values().append_value("a, b");
    left.append(true);
    left.values().append_value("\\N");
    left.append(true);
    let mut right = ListBuilder::new(StringBuilder::new());
    right.values().append_value("a");
    right.values().append_value("b");
    right.append(true);
    right.values().append_null();
    right.append(true);
    let left: ArrayRef = Arc::new(left.finish());
    let right: ArrayRef = Arc::new(right.finish());
    assert_ne!(left.to_data(), right.to_data());
    let actual = eval(
        "nullif",
        vec![left.clone(), right],
        left.data_type().clone(),
    )
    .unwrap();
    // Display strings collide even though both pairs are structurally unequal.
    assert_eq!(actual.null_count(), 2);
    assert_eq!(actual.data_type(), left.data_type());
}
