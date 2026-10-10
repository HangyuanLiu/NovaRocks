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

//! Permanent full-binding-domain probes. The actual resolver admits ANY in
//! every logical position; the signature text is not a numeric admission cap.
use super::aggregate::{AggregateDiffSpec, assert_aggregate_matches_v1};
use super::generate::{InputGenerator, InputProfile};
use arrow::array::{ArrayRef, BinaryArray, Float64Array, Int64Array, ListArray, new_null_array};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Fields, IntervalUnit, TimeUnit};
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
use std::sync::Arc;
fn repeat(value: f64, rows: usize) -> ArrayRef {
    Arc::new(Float64Array::from(vec![value; rows]))
}
fn base(arity: usize, weighted: bool, rows: usize) -> Vec<(FunctionValueType, ArrayRef)> {
    let mut args = vec![(
        FunctionValueType::new(DataType::Float64, false),
        repeat(3.0, rows),
    )];
    if weighted {
        args.push((
            FunctionValueType::new(DataType::Int64, false),
            Arc::new(Int64Array::from(vec![2; rows])),
        ));
    }
    args.push((
        FunctionValueType::new(DataType::Float64, false),
        repeat(0.5, rows),
    ));
    if args.len() < arity {
        args.push((
            FunctionValueType::new(DataType::Int64, false),
            Arc::new(Int64Array::from(vec![2048; rows])),
        ));
    }
    args
}
fn check(name: &str, args: Vec<(FunctionValueType, ArrayRef)>, rows: usize, distinct: bool) {
    let mut spec = AggregateDiffSpec::new(name)
        .grouped((0..rows).map(|row| row % 3).collect(), 4)
        .partitions(3, 391723);
    for (ty, a) in args {
        spec = spec.typed_column(ty, a);
    }
    if distinct {
        spec = spec.original_state_interpretation(true, vec![]);
    }
    assert_aggregate_matches_v1(spec);
}
fn scalar_shapes() -> Vec<DataType> {
    let mut types = vec![
        DataType::Null,
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float32,
        DataType::Float64,
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Binary,
        DataType::LargeBinary,
        DataType::FixedSizeBinary(16),
        DataType::Date32,
    ];
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        for zone in [None, Some("UTC".into()), Some("Asia/Shanghai".into())] {
            types.push(DataType::Timestamp(unit, zone));
        }
    }
    for (max, wide) in [(38u8, false), (76u8, true)] {
        for p in 1..=max {
            for s in [-128, -(max as i8), -2, -1, 0, p as i8] {
                types.push(if wide {
                    DataType::Decimal256(p, s)
                } else {
                    DataType::Decimal128(p, s)
                });
            }
        }
        // Every signed-scale class is exercised at maximum precision, in
        // addition to all precisions and their original arithmetic boundaries.
        for s in i8::MIN..=max as i8 {
            types.push(if wide {
                DataType::Decimal256(max, s)
            } else {
                DataType::Decimal128(max, s)
            });
        }
    }
    types
}
fn generated(ty: &FunctionValueType, rows: usize) -> ArrayRef {
    if ty.data_type == DataType::FixedSizeBinary(16) {
        // The generator has no Physical fixed-binary author. These exact raw
        // bytes are produced by the existing LARGEINT encoder; the explicit
        // selected FVT remains Physical, and separately tests nominal tags.
        return novarocks_functions::largeint::array_from_i128(
            &(0..rows)
                .map(|i| match i % 6 {
                    0 => Some(i128::MIN),
                    1 => Some(i128::MAX),
                    2 => None,
                    3 => Some(-1),
                    4 => Some(0),
                    _ => Some(1),
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();
    }
    InputGenerator::new(98213).column(ty, rows, &InputProfile::default())
}
fn all_roles(name: &str, arity: usize, weighted: bool) {
    let rows = 9;
    for role in 0..arity {
        for dtype in scalar_shapes() {
            let ty = FunctionValueType::new(dtype.clone(), true);
            for values in [
                generated(&ty, rows),
                new_null_array(&dtype, rows),
                new_null_array(&dtype, 0),
                generated(&ty, rows + 4).slice(2, rows),
            ] {
                let count = values.len();
                let mut args = base(arity, weighted, count);
                args[role] = (ty.clone(), values);
                check(name, args, count, false);
            }
        }
    }
    // Other scalar and recursive carriers retain their exact ANY input type.
    // Their original numeric-reader failure precedes NULL payload masking,
    // except that quantile List has its separate original root-NULL branch.
    let nested = [
        DataType::Float16,
        DataType::Date64,
        DataType::Time32(TimeUnit::Second),
        DataType::Time32(TimeUnit::Millisecond),
        DataType::Time64(TimeUnit::Microsecond),
        DataType::Time64(TimeUnit::Nanosecond),
        DataType::Duration(TimeUnit::Second),
        DataType::Duration(TimeUnit::Millisecond),
        DataType::Duration(TimeUnit::Microsecond),
        DataType::Duration(TimeUnit::Nanosecond),
        DataType::Interval(IntervalUnit::YearMonth),
        DataType::Interval(IntervalUnit::DayTime),
        DataType::Interval(IntervalUnit::MonthDayNano),
        DataType::FixedSizeBinary(1),
        DataType::FixedSizeBinary(8),
        DataType::FixedSizeBinary(17),
        DataType::List(Arc::new(Field::new("nested", DataType::Boolean, true))),
        DataType::LargeList(Arc::new(Field::new("wide", DataType::Utf8, true))),
        DataType::FixedSizeList(Arc::new(Field::new("fixed", DataType::Int64, true)), 3),
        DataType::Struct(Fields::from(vec![
            Field::new("state-name", DataType::Utf8, true)
                .with_metadata([("opaque".into(), "original".into())].into()),
        ])),
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int64, true),
                ])),
                false,
            )),
            true,
        ),
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
    ];
    for dtype in nested {
        for role in 0..arity {
            let mut args = base(arity, weighted, rows);
            args[role] = (
                FunctionValueType::new(dtype.clone(), true),
                new_null_array(&dtype, rows),
            );
            check(name, args, rows, false);
        }
    }
    // Actual non-NULL recursive carriers exercise the consumed List-element
    // branch as well as the numeric reader's unsupported-carrier errors.
    use crate::exec::expr::agg::{AggScalarValue as V, build_agg_scalar_array};
    for (dtype, value) in [
        (
            DataType::List(Arc::new(Field::new(
                "authored-bool",
                DataType::Boolean,
                true,
            ))),
            V::List(vec![Some(V::Bool(true)), None]),
        ),
        (
            DataType::List(Arc::new(Field::new("authored-i64", DataType::Int64, true))),
            V::List(vec![Some(V::Int64(0)), Some(V::Int64(1))]),
        ),
        (
            DataType::Struct(Fields::from(vec![Field::new("name", DataType::Utf8, true)])),
            V::Struct(vec![Some(V::Utf8("escaped \" \\ \n".into()))]),
        ),
        (
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Int64, true),
                    ])),
                    false,
                )),
                false,
            ),
            V::Map(vec![(Some(V::Utf8("k".into())), Some(V::Int64(7)))]),
        ),
    ] {
        let values =
            build_agg_scalar_array(&dtype, vec![Some(value.clone()), None, Some(value)]).unwrap();
        for role in 0..arity {
            let mut args = base(arity, weighted, values.len());
            args[role] = (FunctionValueType::new(dtype.clone(), true), values.clone());
            check(name, args, values.len(), false);
        }
    }
    for (logical, dtype) in [
        (ValueLogicalType::Json, DataType::Utf8),
        (ValueLogicalType::Variant, DataType::LargeBinary),
        (ValueLogicalType::Hll, DataType::Binary),
        (ValueLogicalType::Bitmap, DataType::Binary),
        (ValueLogicalType::Object, DataType::LargeBinary),
        (ValueLogicalType::Percentile, DataType::Binary),
        (ValueLogicalType::LargeInt, DataType::FixedSizeBinary(16)),
        (ValueLogicalType::Uuid, DataType::FixedSizeBinary(16)),
    ] {
        let ty = FunctionValueType::try_with_logical_type(dtype.clone(), true, logical).unwrap();
        for role in 0..arity {
            // Selected nominal facts are explicit. The same physical builder
            // supplies malformed/non-NULL payloads; it never guesses a tag.
            for values in [
                generated(&FunctionValueType::new(dtype.clone(), true), rows),
                new_null_array(&dtype, rows),
            ] {
                let mut args = base(arity, weighted, rows);
                args[role] = (ty.clone(), values);
                check(name, args, rows, false);
            }
        }
    }
    // DISTINCT reaches the actual authored state receipt; the original kind
    // does not implement a separate deduplication formula.
    check(name, base(arity, weighted, rows), rows, true);
    let quantile_role = if weighted { 2 } else { 1 };
    for q in [
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        -0.5,
        1.5,
        -0.0,
        0.0,
        0.5,
        1.0,
    ] {
        let mut args = base(arity, weighted, rows);
        args[0] = (
            FunctionValueType::new(DataType::Float64, true),
            new_null_array(&DataType::Float64, rows),
        );
        args[quantile_role] = (
            FunctionValueType::new(DataType::Float64, false),
            repeat(q, rows),
        );
        check(name, args, rows, false);
    }
    for values in [
        vec![
            0.0,
            0.0,
            1.0,
            1.0,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            -0.0,
            4.0,
        ],
        vec![
            16777216.0, 16777217.0, 16777218.0, 1.0e-200, 1.0e200, -1.0e200, 3.0, 3.0, 3.0,
        ],
    ] {
        let mut args = base(arity, weighted, rows);
        args[0] = (
            FunctionValueType::new(DataType::Float64, false),
            Arc::new(Float64Array::from(values)),
        );
        check(name, args, rows, false);
    }
    if weighted {
        for w in [
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            -8.0,
            -0.0,
            0.0,
            0.5,
            1.5,
            i64::MAX as f64,
        ] {
            let mut args = base(arity, weighted, rows);
            args[1] = (
                FunctionValueType::new(DataType::Float64, false),
                repeat(w, rows),
            );
            check(name, args, rows, false);
        }
    }
    if arity > if weighted { 3 } else { 2 } {
        for compression in [
            f64::NAN,
            f64::INFINITY,
            0.0,
            -1.0,
            1.0,
            2047.0,
            2048.0,
            2048.5,
            10000.0,
            10001.0,
        ] {
            let mut args = base(arity, weighted, rows);
            args[arity - 1] = (
                FunctionValueType::new(DataType::Float64, false),
                repeat(compression, rows),
            );
            check(name, args, rows, false);
        }
    }
    // List quantiles preserve order, NULL-element errors, root NULL masking,
    // scalar/array mismatch, count admission and empty-digest IEEE output.
    for q in [
        vec![],
        vec![Some(0.0), Some(1.0), Some(0.5)],
        vec![Some(0.5), None],
        vec![Some(1.25)],
        vec![Some(0.5); 4096],
        vec![Some(0.5); 4097],
    ] {
        let count = q.len();
        let mut flat = vec![];
        let mut offsets = vec![0i32];
        for _ in 0..rows {
            flat.extend(q.iter().copied());
            offsets.push(flat.len() as i32);
        }
        let field = Arc::new(Field::new("authored-quantile", DataType::Float64, true));
        let dtype = DataType::List(field.clone());
        let a = Arc::new(ListArray::new(
            field,
            OffsetBuffer::new(offsets.into()),
            Arc::new(Float64Array::from(flat)),
            None,
        )) as ArrayRef;
        assert_eq!(
            a.as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value(0)
                .len(),
            count
        );
        let mut args = base(arity, weighted, rows);
        args[quantile_role] = (FunctionValueType::new(dtype, true), a);
        check(name, args, rows, false);
    }
    let dtype = DataType::List(Arc::new(Field::new(
        "authored-quantile",
        DataType::Float64,
        true,
    )));
    let mut args = base(arity, weighted, rows);
    args[quantile_role] = (
        FunctionValueType::new(dtype.clone(), true),
        new_null_array(&dtype, rows),
    );
    check(name, args, rows, false);
    // Original bounded-state value route, including its missing magic check.
    for payload in [
        &[0, 4, 0, 0x10, 0x27, 0, 0, 0, 0, 0, 0][..],
        &[0xa2, 3][..],
        &[0xa2, 4][..],
        &[][..],
    ] {
        let mut args = base(arity, weighted, rows);
        args[0] = (
            FunctionValueType::new(DataType::Binary, false),
            Arc::new(BinaryArray::from(vec![payload; rows])),
        );
        check(name, args, rows, false);
    }
    // Real constant pool materialization, rather than a guessed lexical fact.
    let constant = super::constant;
    let mut spec = AggregateDiffSpec::new(name)
        .constant(constant(
            FunctionValueType::new(DataType::Float64, false),
            repeat(3.0, 1),
        ))
        .constant_rows(11);
    if weighted {
        spec = spec.constant(constant(
            FunctionValueType::new(DataType::Int64, false),
            Arc::new(Int64Array::from(vec![2])),
        ));
    }
    spec = spec.constant(constant(
        FunctionValueType::new(DataType::Float64, false),
        repeat(0.5, 1),
    ));
    if arity > if weighted { 3 } else { 2 } {
        spec = spec.constant(constant(
            FunctionValueType::new(DataType::Int64, false),
            Arc::new(Int64Array::from(vec![2048])),
        ));
    }
    assert_aggregate_matches_v1(spec.partitions(4, 1981));
    // A nonzero constant ordinal is a real authored pool projection.
    let ty = FunctionValueType::new(DataType::Float64, false);
    let values: ArrayRef = Arc::new(Float64Array::from(vec![900.0, 3.0, 999.0]));
    let pool = novarocks_functions::ConstantPool::try_new(
        Arc::new(ty.try_to_field("authored-value").unwrap()),
        ty,
        values.to_data(),
        super::constant_policy(),
        novarocks_type_contract::CompilePhase::FunctionSpecialization,
        &super::HarnessControl,
    )
    .unwrap();
    let mut spec = AggregateDiffSpec::new(name)
        .constant(pool.value(1).unwrap())
        .constant_rows(11);
    if weighted {
        spec = spec.constant(constant(
            FunctionValueType::new(DataType::Int64, false),
            Arc::new(Int64Array::from(vec![2])),
        ));
    }
    spec = spec.constant(constant(
        FunctionValueType::new(DataType::Float64, false),
        repeat(0.5, 1),
    ));
    if arity > if weighted { 3 } else { 2 } {
        spec = spec.constant(constant(
            FunctionValueType::new(DataType::Int64, false),
            Arc::new(Int64Array::from(vec![2048])),
        ));
    }
    assert_aggregate_matches_v1(spec.partitions(4, 1982));
}
#[test]
fn approx_percentile_differential_unweighted_arity2_full_actual_declaration() {
    all_roles("percentile_approx", 2, false);
}
#[test]
fn approx_percentile_differential_unweighted_arity3_full_actual_declaration() {
    all_roles("percentile_approx", 3, false);
}
#[test]
fn approx_percentile_differential_weighted_arity3_full_actual_declaration() {
    all_roles("percentile_approx_weighted", 3, true);
}
#[test]
fn approx_percentile_differential_weighted_arity4_full_actual_declaration() {
    all_roles("percentile_approx_weighted", 4, true);
}

#[path = "pure_differential_approx_percentile_remaining_carriers.rs"]
mod remaining_carriers;
