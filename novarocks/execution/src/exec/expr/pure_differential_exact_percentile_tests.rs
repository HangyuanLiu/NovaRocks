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

//! Permanent exact declaration-domain comparisons; run before owner install.
use super::aggregate::{AggregateDiffSpec, assert_aggregate_matches_v1};
use super::generate::{InputGenerator, InputProfile};
use arrow::array::{ArrayRef, Float64Array, new_null_array};
use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use novarocks_type_contract::FunctionValueType;
use std::sync::Arc;
fn check(name: &str, ty: DataType, values: ArrayRef, rate: f64) {
    let rows = values.len();
    assert_aggregate_matches_v1(
        AggregateDiffSpec::new(name)
            .typed_column(FunctionValueType::new(ty, true), values)
            .typed_column(
                FunctionValueType::new(DataType::Float64, false),
                Arc::new(Float64Array::from(vec![rate; rows])),
            )
            .grouped((0..rows).map(|row| row % 3).collect(), 4)
            .partitions(5, 9827),
    );
}
fn complete_original_shapes(name: &str) {
    let mut shapes = vec![
        DataType::Null,
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Utf8,
        DataType::Binary,
        DataType::LargeBinary,
        DataType::LargeUtf8,
        DataType::FixedSizeBinary(16),
        DataType::Date32,
    ];
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        for tz in [None, Some("UTC".into()), Some("Asia/Shanghai".into())] {
            shapes.push(DataType::Timestamp(unit, tz));
        }
    }
    for (max, wide) in [(38u8, false), (76u8, true)] {
        for precision in 1..=max {
            for scale in [-128, -76, -38, -2, 0, precision as i8] {
                shapes.push(if wide {
                    DataType::Decimal256(precision, scale)
                } else {
                    DataType::Decimal128(precision, scale)
                });
            }
        }
    }
    for shape in shapes {
        let ty = FunctionValueType::new(shape.clone(), true);
        let values = if shape == DataType::FixedSizeBinary(16) {
            // The generic Physical generator has no fixed-width binary author.
            // Keep the admitted original carrier and use its real byte builder.
            novarocks_functions::largeint::array_from_i128(&[
                None,
                Some(i128::MIN),
                Some(i128::MAX),
                Some(-1),
                Some(0),
                Some(1),
                Some(i64::MIN as i128),
                Some(i64::MAX as i128),
                None,
                Some(-17),
                Some(17),
                Some(1_i128 << 64),
                Some(-(1_i128 << 64)),
                Some(42),
                Some(-42),
                None,
                Some(7),
            ])
            .unwrap()
        } else {
            InputGenerator::new(9871).column(&ty, 17, &InputProfile::default())
        };
        for rate in [0.0, 0.25, 0.5, 1.0] {
            check(name, shape.clone(), values.clone(), rate);
        }
        for rows in [0, 17] {
            check(name, shape.clone(), new_null_array(&shape, rows), 0.5);
        }
    }
    // ANY admits recursive source values. The original scalar reader and
    // validator produce their original data errors, rather than a safe subset.
    use crate::exec::expr::agg::{AggScalarValue as V, build_agg_scalar_array};
    let recursive = [
        (
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            V::List(vec![Some(V::Int64(3)), None]),
        ),
        (
            DataType::Struct(Fields::from(vec![Field::new("x", DataType::Utf8, true)])),
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
    ];
    for (ty, value) in recursive {
        let values = build_agg_scalar_array(&ty, vec![Some(value), None]).unwrap();
        check(name, ty, values, 0.5);
    }
    // IEEE values expose original JSON null encoding and comparator rules.
    check(
        name,
        DataType::Float64,
        Arc::new(Float64Array::from(vec![
            Some(-0.0),
            Some(0.0),
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            None,
        ])),
        0.5,
    );
    for rate in [-0.25, 1.25, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        check(
            name,
            DataType::Float64,
            Arc::new(Float64Array::from(vec![None, None])),
            rate,
        );
    }
}
#[test]
fn exact_percentile_differential_cont_full_declared_shape_and_original_error_domain() {
    complete_original_shapes("percentile_cont");
}
#[test]
fn exact_percentile_differential_disc_full_declared_shape_and_original_error_domain() {
    complete_original_shapes("percentile_disc");
}
#[test]
fn exact_percentile_differential_lc_full_declared_shape_and_original_error_domain() {
    complete_original_shapes("percentile_disc_lc");
}
