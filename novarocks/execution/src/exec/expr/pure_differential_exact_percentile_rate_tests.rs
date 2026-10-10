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

//! Actual second-argument ANY domain, not an invented Float64 constraint.
use super::aggregate::{AggregateDiffSpec, assert_aggregate_matches_v1};
use super::generate::{InputGenerator, InputProfile};
use arrow::array::{ArrayRef, Float64Array, Int64Array, new_null_array};
use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
use std::sync::Arc;
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
fn check(name: &str, rate_type: FunctionValueType, rates: ArrayRef, value_null: bool) {
    let rows = rates.len();
    let values: ArrayRef = if value_null {
        new_null_array(&DataType::Float64, rows)
    } else {
        Arc::new(Float64Array::from(vec![3.0; rows]))
    };
    assert_aggregate_matches_v1(
        AggregateDiffSpec::new(name)
            .typed_column(
                FunctionValueType::new(DataType::Float64, value_null),
                values,
            )
            .typed_column(rate_type, rates)
            .grouped((0..rows).map(|r| r % 3).collect(), 4)
            .partitions(3, 92015),
    );
}
fn domain(name: &str) {
    for dtype in scalar_shapes() {
        let ty = FunctionValueType::new(dtype.clone(), true);
        for values in [
            generated(&ty, 9),
            new_null_array(&dtype, 9),
            new_null_array(&dtype, 0),
            generated(&ty, 13).slice(2, 9),
        ] {
            for value_null in [false, true] {
                check(name, ty.clone(), values.clone(), value_null);
            }
        }
    }
    use crate::exec::expr::agg::{AggScalarValue as V, build_agg_scalar_array};
    for (dtype, value) in [
        (
            DataType::List(Arc::new(Field::new("authored", DataType::Int64, true))),
            V::List(vec![Some(V::Int64(1)), None]),
        ),
        (
            DataType::Struct(Fields::from(vec![Field::new("x", DataType::Utf8, true)])),
            V::Struct(vec![Some(V::Utf8("q".into()))]),
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
            V::Map(vec![(Some(V::Utf8("k".into())), Some(V::Int64(1)))]),
        ),
    ] {
        let values = build_agg_scalar_array(&dtype, vec![Some(value), None]).unwrap();
        for value_null in [false, true] {
            check(
                name,
                FunctionValueType::new(dtype.clone(), true),
                values.clone(),
                value_null,
            );
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
        for value_null in [false, true] {
            check(
                name,
                ty.clone(),
                generated(&FunctionValueType::new(dtype.clone(), true), 9),
                value_null,
            );
        }
    }
    // Actual SQL 0/1 integer and decimal constants are separate source facts.
    for rate in [0, 1] {
        let values = Arc::new(Int64Array::from(vec![rate])) as ArrayRef;
        let constant = super::constant(FunctionValueType::new(DataType::Int64, false), values);
        assert_aggregate_matches_v1(
            AggregateDiffSpec::new(name)
                .column(Arc::new(Float64Array::from(vec![3.0; 11])))
                .constant(constant)
                .partitions(3, 713),
        );
    }
    for (p, s, value) in [(2, 1, 5), (3, 2, 25), (3, 2, 75)] {
        let rates = Arc::new(
            arrow::array::Decimal128Array::from(vec![value])
                .with_precision_and_scale(p, s)
                .unwrap(),
        ) as ArrayRef;
        let constant = super::constant(
            FunctionValueType::new(DataType::Decimal128(p, s), false),
            rates,
        );
        assert_aggregate_matches_v1(
            AggregateDiffSpec::new(name)
                .column(Arc::new(Float64Array::from(vec![3.0; 11])))
                .constant(constant)
                .partitions(3, 713),
        );
    }
}
#[test]
fn exact_percentile_rate_full_domain_cont() {
    domain("percentile_cont");
}
#[test]
fn exact_percentile_rate_full_domain_disc() {
    domain("percentile_disc");
}
#[test]
fn exact_percentile_rate_full_domain_lc() {
    domain("percentile_disc_lc");
}
