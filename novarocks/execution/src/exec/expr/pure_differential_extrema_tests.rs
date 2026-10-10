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
//! Every admitted greatest/least carrier uses the unchanged eager v1 oracle.
use super::*;
use arrow::array::{
    Date32Array, Decimal128Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, NullArray, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};

fn compare_profiles(left: ArrayRef, right: ArrayRef, seed: u64) {
    for name in ["greatest", "least"] {
        assert_scalar_matches_v1(
            ScalarDiffSpec::new(name)
                .column(left.clone())
                .column(right.clone())
                .sparse_selections(12, seed),
        );
        if left.len() > 2 {
            assert_scalar_matches_v1(
                ScalarDiffSpec::new(name)
                    .column(left.slice(1, left.len() - 2))
                    .column(right.slice(1, right.len() - 2))
                    .sparse_selections(12, seed + 1),
            );
        }
    }
}
#[test]
fn pure_differential_extrema_every_signed_and_float_carrier() {
    macro_rules! integer {
        ($array:ty,$seed:expr) => {
            compare_profiles(
                Arc::new(<$array>::from(vec![
                    Some(1),
                    Some(9),
                    None,
                    Some(-4),
                    Some(0),
                ])),
                Arc::new(<$array>::from(vec![
                    Some(2),
                    Some(8),
                    Some(3),
                    None,
                    Some(0),
                ])),
                $seed,
            );
        };
    }
    integer!(Int8Array, 700);
    integer!(Int16Array, 710);
    integer!(Int32Array, 720);
    integer!(Int64Array, 730);
    compare_profiles(
        Arc::new(Float32Array::from(vec![
            Some(-0.0),
            Some(f32::NAN),
            Some(f32::INFINITY),
            Some(-2.5),
            None,
        ])),
        Arc::new(Float32Array::from(vec![
            Some(0.0),
            Some(2.0),
            Some(2.0),
            Some(-3.0),
            Some(1.0),
        ])),
        740,
    );
    compare_profiles(
        Arc::new(Float64Array::from(vec![
            Some(-0.0),
            Some(f64::NAN),
            Some(f64::NEG_INFINITY),
            Some(-2.5),
            None,
        ])),
        Arc::new(Float64Array::from(vec![
            Some(0.0),
            Some(2.0),
            Some(2.0),
            Some(-3.0),
            Some(1.0),
        ])),
        750,
    );
    for name in ["greatest", "least"] {
        assert_scalar_matches_v1(
            ScalarDiffSpec::new(name)
                .column(Arc::new(Int64Array::from(vec![
                    Some(9_007_199_254_740_993),
                    Some(i64::MAX),
                    Some(i64::MIN),
                    None,
                ])))
                .sparse_selections(12, 751),
        );
        assert_scalar_matches_v1(
            ScalarDiffSpec::new(name)
                .column(Arc::new(Int64Array::from(vec![
                    Some(1),
                    None,
                    Some(-5),
                    Some(0),
                ])))
                .column(Arc::new(Float64Array::from(vec![
                    Some(1.5),
                    Some(2.0),
                    Some(-6.0),
                    Some(-0.0),
                ])))
                .sparse_selections(12, 752),
        );
    }
}
#[test]
fn pure_differential_extrema_decimal_metadata_and_widened_decimal256_result() {
    for (precision, scale) in [(9, 0), (18, 2), (38, -46)] {
        compare_profiles(
            Arc::new(
                Decimal128Array::from(vec![Some(1), Some(-2), None, Some(0), Some(4)])
                    .with_precision_and_scale(precision, scale)
                    .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from(vec![Some(2), Some(-1), Some(3), Some(0), None])
                    .with_precision_and_scale(precision, scale)
                    .unwrap(),
            ),
            760,
        );
    }
    compare_profiles(
        Arc::new(
            Decimal128Array::from(vec![Some(1), Some(-1), None, Some(0), Some(4)])
                .with_precision_and_scale(38, 0)
                .unwrap(),
        ),
        Arc::new(
            Decimal128Array::from(vec![Some(20), Some(-20), Some(3), Some(0), None])
                .with_precision_and_scale(38, 1)
                .unwrap(),
        ),
        761,
    );
    let maximum = 10_i128.pow(38) - 1;
    compare_profiles(
        Arc::new(
            Decimal128Array::from(vec![Some(maximum), Some(-maximum), Some(0), None])
                .with_precision_and_scale(38, -46)
                .unwrap(),
        ),
        Arc::new(
            Decimal128Array::from(vec![Some(maximum), Some(-maximum), None, Some(0)])
                .with_precision_and_scale(38, -46)
                .unwrap(),
        ),
        762,
    );
}
#[test]
fn pure_differential_extrema_date_utf8_and_all_timestamp_units() {
    compare_profiles(
        Arc::new(Date32Array::from(vec![
            Some(-1),
            Some(0),
            Some(i32::MAX),
            None,
            Some(1),
        ])),
        Arc::new(Date32Array::from(vec![
            Some(0),
            Some(-1),
            Some(1),
            Some(2),
            None,
        ])),
        770,
    );
    compare_profiles(
        Arc::new(StringArray::from(vec![
            Some("20260114"),
            Some("2026-01-13 20:01:02.999"),
            Some("z"),
            None,
            Some("20260101"),
        ])),
        Arc::new(StringArray::from(vec![
            Some("2026-01-11"),
            Some("2026-01-14 00:00:00"),
            Some("a"),
            Some("20260101"),
            None,
        ])),
        771,
    );
    macro_rules! timestamp {
        ($array:ty,$seed:expr) => {
            compare_profiles(
                Arc::new(<$array>::from(vec![
                    Some(-1),
                    Some(1001),
                    Some(1234567899),
                    None,
                    Some(0),
                ])),
                Arc::new(<$array>::from(vec![
                    Some(-1001),
                    Some(999),
                    Some(1234567891),
                    Some(0),
                    None,
                ])),
                $seed,
            );
            compare_profiles(
                Arc::new(
                    <$array>::from(vec![Some(-1), Some(1001), Some(1234567899), None, Some(0)])
                        .with_timezone("+02:00"),
                ),
                Arc::new(
                    <$array>::from(vec![
                        Some(-1001),
                        Some(999),
                        Some(1234567891),
                        Some(0),
                        None,
                    ])
                    .with_timezone("+02:00"),
                ),
                $seed + 1,
            );
        };
    }
    timestamp!(TimestampSecondArray, 780);
    timestamp!(TimestampMillisecondArray, 782);
    timestamp!(TimestampMicrosecondArray, 784);
    timestamp!(TimestampNanosecondArray, 786);
}
#[test]
fn pure_differential_extrema_null_broadcast_variadic_empty_and_long_parser_work() {
    for name in ["greatest", "least"] {
        // The real Null result requires an unsupported legacy Timestamp->Null cast.
        // Its full raw error and exact preparation refusal have a separate oracle.
        assert_scalar_matches_v1(
            ScalarDiffSpec::new(name)
                .column(Arc::new(NullArray::new(5)))
                .column(Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])))
                .sparse_selections(12, 790),
        );
        assert_scalar_matches_v1(
            ScalarDiffSpec::new(name)
                .column(Arc::new(Int64Array::from(vec![
                    Some(1),
                    Some(3),
                    None,
                    Some(-1),
                    Some(4),
                ])))
                .constant_array(Arc::new(Int64Array::from(vec![2])))
                .column(Arc::new(Int64Array::from(vec![
                    Some(4),
                    None,
                    Some(0),
                    Some(3),
                    Some(-3),
                ])))
                .sparse_selections(12, 791),
        );
        assert_scalar_matches_v1(
            ScalarDiffSpec::new(name)
                .column(Arc::new(Float64Array::from(Vec::<Option<f64>>::new())))
                .sparse_selections(12, 792),
        );
        let long = "invalid_datetime_".to_string() + &"x".repeat(3000);
        assert_scalar_matches_v1(
            ScalarDiffSpec::new(name)
                .column(Arc::new(StringArray::from(vec![
                    Some(long.as_str()),
                    Some("2026-01-14 12:00:00"),
                    None,
                ])))
                .constant_array(Arc::new(StringArray::from(vec!["20260115"])))
                .sparse_selections(12, 793),
        );
    }
}

#[test]
fn pure_differential_extrema_declared_json_source_physical_result() {
    let json = FunctionValueType::try_with_logical_type(
        DataType::Utf8,
        true,
        novarocks_type_contract::ValueLogicalType::Json,
    )
    .unwrap();
    let physical = FunctionValueType::new(DataType::Utf8, true);
    let json_values = Arc::new(StringArray::from(vec![
        Some("20260114"),
        Some("20260111"),
        Some("null"),
        None,
        Some("20260101"),
    ])) as ArrayRef;
    let physical_values = Arc::new(StringArray::from(vec![
        Some("2026-01-11"),
        Some("2026-01-14 00:00:00"),
        Some("20260101"),
        Some("20260101"),
        None,
    ])) as ArrayRef;
    for name in ["greatest", "least"] {
        assert_scalar_matches_v1(
            ScalarDiffSpec::new(name)
                .typed_column(json.clone(), json_values.clone())
                .typed_column(physical.clone(), physical_values.clone())
                .sparse_selections(12, 795),
        );
        assert_scalar_matches_v1(
            ScalarDiffSpec::new(name)
                .typed_column(physical.clone(), physical_values.clone())
                .typed_column(json.clone(), json_values.clone())
                .sparse_selections(12, 796),
        );
    }
}
