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

//! Original GROUP_CONCAT scalar profiles and explicit cross-phase ordering facts.
use super::aggregate::{AggregateDiffSpec, assert_aggregate_matches_v1};
use super::{DiffSemantics, FloatComparison};
use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    StringArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, new_null_array,
};
use arrow::datatypes::DataType;
use arrow_buffer::i256;
use novarocks_type_contract::SemanticParameterValue;
use std::sync::Arc;
fn semantics(raw_limit: i64, mode: bool) -> DiffSemantics {
    DiffSemantics {
        extra: vec![
            SemanticParameterValue::GroupConcatLegacy(mode),
            SemanticParameterValue::GroupConcatMaxLen(raw_limit),
        ],
        ..Default::default()
    }
}
fn values() -> Vec<ArrayRef> {
    let mut arrays: Vec<ArrayRef> = vec![
        Arc::new(BooleanArray::from(vec![
            Some(true),
            None,
            Some(false),
            Some(true),
        ])),
        Arc::new(Int8Array::from(vec![
            Some(i8::MIN),
            None,
            Some(0),
            Some(i8::MAX),
        ])),
        Arc::new(Int16Array::from(vec![
            Some(i16::MIN),
            None,
            Some(0),
            Some(i16::MAX),
        ])),
        Arc::new(Int32Array::from(vec![
            Some(i32::MIN),
            None,
            Some(0),
            Some(i32::MAX),
        ])),
        Arc::new(Int64Array::from(vec![
            Some(i64::MIN),
            None,
            Some(0),
            Some(i64::MAX),
        ])),
        Arc::new(Float32Array::from(vec![
            Some(f32::NEG_INFINITY),
            None,
            Some(-0.0),
            Some(f32::INFINITY),
        ])),
        Arc::new(Float64Array::from(vec![
            Some(f64::NEG_INFINITY),
            None,
            Some(-0.0),
            Some(f64::INFINITY),
        ])),
        Arc::new(StringArray::from(vec![
            Some("é你"),
            None,
            Some(""),
            Some("abc"),
        ])),
        Arc::new(Date32Array::from(vec![Some(-1), None, Some(0), Some(1)])),
        Arc::new(BinaryArray::from(vec![
            Some(&[0, 255][..]),
            None,
            Some(&[][..]),
            Some(&[7][..]),
        ])),
        Arc::new(LargeBinaryArray::from(vec![
            Some(&[0, 255][..]),
            None,
            Some(&[][..]),
            Some(&[7][..]),
        ])),
        new_null_array(&DataType::Null, 4),
        novarocks_types::largeint::array_from_i128(&[
            Some(i128::MIN),
            None,
            Some(0),
            Some(i128::MAX),
        ])
        .unwrap(),
    ];
    for zone in [None, Some("UTC"), Some("Asia/Shanghai")] {
        macro_rules! ts {
            ($array:ty) => {{
                let arr = <$array>::from(vec![Some(-1), None, Some(0), Some(i64::MAX)]);
                match zone {
                    Some(zone) => Arc::new(arr.with_timezone(zone)) as ArrayRef,
                    None => Arc::new(arr) as ArrayRef,
                }
            }};
        }
        arrays.extend([
            ts!(TimestampSecondArray),
            ts!(TimestampMillisecondArray),
            ts!(TimestampMicrosecondArray),
            ts!(TimestampNanosecondArray),
        ]);
    }
    for scale in [-2, 0, 2] {
        arrays.push(Arc::new(
            Decimal128Array::from(vec![Some(-123), None, Some(0), Some(123)])
                .with_precision_and_scale(38, scale)
                .unwrap(),
        ));
        arrays.push(Arc::new(
            Decimal256Array::from(vec![
                Some(i256::from_i128(-123)),
                None,
                Some(i256::ZERO),
                Some(i256::from_i128(123)),
            ])
            .with_precision_and_scale(76, scale)
            .unwrap(),
        ));
    }
    arrays
}
#[test]
fn pure_differential_concat_scalar_profiles_dynamic_separators_and_limits() {
    for name in ["group_concat", "string_agg"] {
        for source in values() {
            for mode in [false, true] {
                for max_len in [-121, 0, 4, 5, 8, 4096] {
                    let summary = assert_aggregate_matches_v1(
                        AggregateDiffSpec::new(name)
                            .original_state_interpretation(false, vec![])
                            .column(source.clone())
                            .column(Arc::new(StringArray::from(vec![
                                Some("é"),
                                None,
                                Some(""),
                                Some(","),
                            ])))
                            .grouped(vec![0, 0, 0, 0], 2)
                            .partitions(3, 1901)
                            .semantics(semantics(max_len, mode))
                            .float_comparison(FloatComparison::Exact),
                    );
                    assert_eq!(
                        summary.matched_failures,
                        0,
                        "{name} {:?}, mode={mode}, limit={max_len}",
                        source.data_type()
                    );
                }
            }
        }
    }
}
#[test]
fn pure_differential_concat_original_invalid_date_and_decimal_overflow_domain() {
    for name in ["group_concat", "string_agg"] {
        let summary = assert_aggregate_matches_v1(
            AggregateDiffSpec::new(name)
                .original_state_interpretation(false, vec![])
                .column(Arc::new(Date32Array::from(vec![i32::MIN])))
                .column(Arc::new(StringArray::from(vec![","])))
                .semantics(semantics(1024, false)),
        );
        assert_eq!(summary.matched_failures, 2);
        if cfg!(debug_assertions) {
            for (array, panic) in [
                (
                    Arc::new(Date32Array::from(vec![i32::MAX])) as ArrayRef,
                    "attempt to add with overflow",
                ),
                (
                    Arc::new(
                        Decimal128Array::from(vec![i128::MIN])
                            .with_precision_and_scale(38, 2)
                            .unwrap(),
                    ) as ArrayRef,
                    "attempt to negate with overflow",
                ),
            ] {
                let mut spec = AggregateDiffSpec::new(name)
                    .original_state_interpretation(false, vec![])
                    .column(array)
                    .column(Arc::new(StringArray::from(vec![","])))
                    .semantics(semantics(1024, false));
                spec.expected_panic_payload = Some(panic.to_owned());
                let summary = assert_aggregate_matches_v1(spec);
                assert_eq!(summary.matched_panics, 2);
            }
        }
    }
}

#[test]
fn pure_differential_concat_multiple_outputs_and_nested_formatter_projection() {
    use arrow::array::{ListArray, StructArray};
    use arrow::datatypes::{Field, Fields};
    use arrow_buffer::OffsetBuffer;
    for name in ["group_concat", "string_agg"] {
        let source = Arc::new(StructArray::new(
            Fields::from(vec![
                Field::new(
                    "ts",
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("decimal", DataType::Decimal128(9, 2), true),
            ]),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![-1, 0, 1])) as ArrayRef,
                Arc::new(
                    Decimal128Array::from(vec![Some(-123), None, Some(123)])
                        .with_precision_and_scale(9, 2)
                        .unwrap(),
                ) as ArrayRef,
            ],
            None,
        )) as ArrayRef;
        let list = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int64, true)),
            OffsetBuffer::new(vec![0i32, 2, 2, 4].into()),
            Arc::new(Int64Array::from(vec![
                Some(i64::MIN),
                None,
                Some(i64::MAX),
                Some(0),
            ])),
            None,
        )) as ArrayRef;
        for source in [source, list] {
            let summary = assert_aggregate_matches_v1(
                AggregateDiffSpec::new(name)
                    .original_state_interpretation(false, vec![])
                    .column(source)
                    .column(Arc::new(StringArray::from(vec!["x", "y", ""])))
                    .column(Arc::new(StringArray::from(vec![",", "|", "/"])))
                    .grouped(vec![0, 0, 0], 2)
                    .semantics(semantics(4096, false)),
            );
            assert_eq!(summary.matched_failures, 0);
        }
    }
}

#[test]
fn pure_differential_concat_original_distinct_order_policy_survives_final() {
    use novarocks_type_contract::AggregateStateOrderKey as K;
    for name in ["group_concat", "string_agg"] {
        for distinct in [false, true] {
            for mode in [false, true] {
                for raw_limit in [-7, 4, 4096] {
                    for (ascending, nulls_first) in
                        [(false, false), (false, true), (true, false), (true, true)]
                    {
                        let spec = AggregateDiffSpec::new(name)
                            .original_state_interpretation(
                                distinct,
                                vec![K {
                                    ascending,
                                    nulls_first,
                                }],
                            )
                            .column(Arc::new(StringArray::from(vec![
                                Some("a"),
                                Some("b"),
                                Some("a"),
                                None,
                                Some("c"),
                            ])))
                            .column(Arc::new(StringArray::from(vec![
                                Some(","),
                                Some("é"),
                                Some("/"),
                                Some("|"),
                                None,
                            ])))
                            .column(Arc::new(Int64Array::from(vec![
                                Some(2),
                                Some(1),
                                Some(2),
                                Some(0),
                                None,
                            ])))
                            .grouped(vec![0, 0, 0, 0, 0], 2)
                            .partitions(3, 773)
                            .semantics(semantics(raw_limit, mode));
                        let summary = assert_aggregate_matches_v1(spec);
                        assert_eq!(summary.matched_failures, 0);
                    }
                }
            }
        }
    }
}
