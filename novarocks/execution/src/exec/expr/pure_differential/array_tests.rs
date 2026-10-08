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
//! Original ARRAY family profiles, selected rows and exact producer ordering.
use super::aggregate::{
    AggregateDiffSpec, assert_aggregate_matches_v1, run_aggregate_differential,
};
use super::generate::{InputGenerator, InputProfile};
use super::{DifferentialFailure, FloatComparison};
use arrow::array::MapArray;
use arrow::array::{
    Array, ArrayRef, Float32Array, Float64Array, Int64Array, ListArray, StringArray, StructArray,
    new_empty_array, new_null_array,
};
use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use novarocks_type_contract::{AggregateStateOrderKey as K, FunctionValueType, ValueLogicalType};
use std::sync::Arc;
const ROWS: usize = 513;
fn profiles(nullable: bool) -> Vec<FunctionValueType> {
    let mut types = vec![
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Utf8,
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
    for precision in [9, 18, 38] {
        for scale in [-2, 0, 2, precision as i8] {
            types.push(DataType::Decimal128(precision, scale));
        }
    }
    types.push(DataType::FixedSizeBinary(16));
    let mut out = types
        .into_iter()
        .map(|ty| FunctionValueType::new(ty, nullable))
        .collect::<Vec<_>>();
    out.push(FunctionValueType {
        logical_type: ValueLogicalType::LargeInt,
        ..FunctionValueType::new(DataType::FixedSizeBinary(16), nullable)
    });
    out
}
fn values(source: &FunctionValueType, rows: usize) -> ArrayRef {
    if source.data_type == DataType::FixedSizeBinary(16) {
        let pattern = [
            Some(i128::MIN),
            Some(i128::MAX),
            Some(-1),
            Some(0),
            Some(1i128 << 80),
            None,
        ];
        return novarocks_types::largeint::array_from_i128(
            &(0..rows)
                .map(|r| pattern[r % pattern.len()].or_else(|| (!source.nullable).then_some(7)))
                .collect::<Vec<_>>(),
        )
        .unwrap();
    }
    InputGenerator::new(2401).column(
        source,
        rows,
        &InputProfile::default().with_boundary_ratio(0.0),
    )
}
fn check(spec: AggregateDiffSpec) {
    let summary = assert_aggregate_matches_v1(spec.float_comparison(FloatComparison::Exact));
    assert_eq!(summary.matched_failures, 0);
    assert_eq!(summary.null_results, 0);
}
#[test]
fn pure_differential_array_every_scalar_nullable_and_logical_profile() {
    for name in ["array_agg", "array_agg_distinct"] {
        for nullable in [false, true] {
            for source in profiles(nullable)
                .into_iter()
                .filter(|source| source.logical_type != ValueLogicalType::LargeInt)
            {
                check(
                    AggregateDiffSpec::new(name)
                        .original_state_interpretation(false, vec![])
                        .typed_column(source.clone(), values(&source, ROWS))
                        .grouped((0..ROWS).map(|r| r % 5).collect(), 7)
                        .partitions(7, 2402),
                );
            }
        }
    }
}
#[test]
fn pure_differential_array_empty_all_null_and_constant_source() {
    for name in ["array_agg", "array_agg_distinct"] {
        for source in profiles(true)
            .into_iter()
            .filter(|source| source.logical_type != ValueLogicalType::LargeInt)
            .chain([FunctionValueType::new(DataType::Null, true)])
        {
            for rows in [0, ROWS] {
                let array = if rows == 0 {
                    new_empty_array(&source.data_type)
                } else {
                    new_null_array(&source.data_type, rows)
                };
                check(
                    AggregateDiffSpec::new(name)
                        .original_state_interpretation(false, vec![])
                        .typed_column(source.clone(), array)
                        .partitions(7, 2403),
                );
            }
        }
        check(
            AggregateDiffSpec::new(name)
                .original_state_interpretation(false, vec![])
                .constant(super::constant(
                    FunctionValueType::new(DataType::Int64, false),
                    Arc::new(Int64Array::from(vec![7])),
                ))
                .constant_rows(ROWS)
                .partitions(7, 2404),
        );
    }
}
fn nested() -> Vec<ArrayRef> {
    let ints: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(7),
        None,
        Some(7),
        Some(-1),
        Some(0),
        Some(7),
    ]));
    let list: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Int64, true)),
        OffsetBuffer::new(vec![0i32, 2, 2, 3, 6].into()),
        ints,
        Some(NullBuffer::from(vec![true, false, true, true])),
    ));
    let strings: ArrayRef = Arc::new(StringArray::from(vec![
        Some("x"),
        None,
        Some("x"),
        Some("你"),
    ]));
    let row: ArrayRef = Arc::new(StructArray::new(
        Fields::from(vec![
            Field::new("a", list.data_type().clone(), true),
            Field::new("b", DataType::Utf8, true),
        ]),
        vec![list.clone(), strings],
        Some(NullBuffer::from(vec![true, true, false, true])),
    ));
    vec![list, row]
}
#[test]
fn pure_differential_array_recursive_list_struct_and_unique_list_nulls() {
    for source in nested() {
        for name in ["array_agg", "array_agg_distinct"] {
            check(
                AggregateDiffSpec::new(name)
                    .original_state_interpretation(false, vec![])
                    .column(source.clone())
                    .grouped(vec![0, 0, 1, 0], 3)
                    .partitions(3, 2405),
            );
        }
        if source.as_any().is::<ListArray>() {
            check(
                AggregateDiffSpec::new("array_unique_agg")
                    .original_state_interpretation(false, vec![])
                    .column(source)
                    .grouped(vec![0, 0, 1, 0], 3)
                    .partitions(3, 2405),
            );
        }
    }
}
#[test]
fn pure_differential_array_order_distinct_producer_receipt_survives_final_empty_flags() {
    for name in ["array_agg", "array_agg_distinct"] {
        for distinct in [false, true] {
            for ascending in [false, true] {
                for nulls_first in [false, true] {
                    check(
                        AggregateDiffSpec::new(name)
                            .original_state_interpretation(
                                distinct,
                                vec![K {
                                    ascending,
                                    nulls_first,
                                }],
                            )
                            .column(Arc::new(Int64Array::from(vec![
                                Some(7),
                                Some(8),
                                Some(7),
                                None,
                                Some(9),
                            ])))
                            .column(Arc::new(Int64Array::from(vec![
                                Some(3),
                                Some(2),
                                Some(1),
                                None,
                                Some(0),
                            ])))
                            .grouped(vec![0, 0, 0, 0, 0], 2)
                            .partitions(3, 2406),
                    );
                }
            }
        }
    }
}
#[test]
fn pure_differential_array_original_nan_fingerprint_and_signed_zero() {
    let doubles: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(f64::from_bits(0x7ff8000000000041)),
        Some(f64::from_bits(0x7ff8000000000042)),
        Some(-0.0),
        Some(0.0),
        Some(f64::NEG_INFINITY),
        Some(f64::INFINITY),
        None,
    ]));
    let floats: ArrayRef = Arc::new(Float32Array::from(vec![
        Some(f32::from_bits(0x7fc00041)),
        Some(f32::from_bits(0x7fc00042)),
        Some(-0.0),
        Some(0.0),
        Some(f32::NEG_INFINITY),
        Some(f32::INFINITY),
        None,
    ]));
    for array in [doubles, floats] {
        for name in ["array_agg", "array_agg_distinct"] {
            check(
                AggregateDiffSpec::new(name)
                    .original_state_interpretation(false, vec![])
                    .column(array.clone())
                    .partitions(3, 2407),
            );
        }
    }
    let summary = assert_aggregate_matches_v1(
        AggregateDiffSpec::new("array_agg")
            .original_state_interpretation(
                false,
                vec![K {
                    ascending: true,
                    nulls_first: false,
                }],
            )
            .column(Arc::new(Int64Array::from(vec![1, 2, 3])))
            .column(Arc::new(Float64Array::from(vec![f64::NAN, 1.0, 0.0]))),
    );
    assert_eq!(summary.matched_failures, 2);
}
#[test]
fn pure_differential_array_unique_nonlist_catalogue_drift_is_named_refusal() {
    for ty in [DataType::Int64, DataType::Utf8, DataType::Boolean] {
        let source = FunctionValueType::new(ty, true);
        let failure = run_aggregate_differential(
            &AggregateDiffSpec::new("array_unique_agg")
                .original_state_interpretation(false, vec![])
                .typed_column(source.clone(), values(&source, 8)),
        )
        .unwrap_err();
        let DifferentialFailure::LegacyUnavailable { name, reason } = failure else {
            panic!("expected original non-List catalogue drift: {failure}")
        };
        assert_eq!(name, "array_unique_agg");
        let scalar = &source.data_type;
        let list = DataType::List(Arc::new(Field::new("item", scalar.clone(), true)));
        assert_eq!(
            reason,
            format!(
                "bind aggregate `array_unique_agg`: prepare aggregate `array_unique_agg` overload `builtin.aggregate/array_unique_agg/derived-v1`: legacy aggregate type drift: implementation intermediate={scalar:?}, output={list:?}; catalog intermediate={scalar:?}, output={scalar:?}"
            )
        );
    }
}

#[test]
fn pure_differential_array_unique_every_accurate_list_element_profile() {
    for nullable in [false, true] {
        for source in profiles(nullable) {
            let elements = values(&source, 12);
            let mut item = Field::new("element", source.data_type.clone(), true);
            if source.logical_type == ValueLogicalType::LargeInt {
                item = item.with_metadata(
                    [("nr_logical_type".to_string(), "largeint".to_string())].into(),
                );
            }
            let list: ArrayRef = Arc::new(ListArray::new(
                Arc::new(item),
                OffsetBuffer::new(vec![0i32, 3, 6, 9, 12].into()),
                elements,
                Some(NullBuffer::from(vec![true, false, true, true])),
            ));
            check(
                AggregateDiffSpec::new("array_unique_agg")
                    .original_state_interpretation(false, vec![])
                    .column(list)
                    .grouped(vec![0, 0, 1, 0], 3)
                    .partitions(3, 2408),
            );
        }
    }
}
#[test]
fn pure_differential_array_recursive_map_and_ordered_struct_values() {
    let fields = Fields::from(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Utf8, true),
    ]);
    let entries = StructArray::new(
        fields.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 1, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("你"),
                None,
                Some("你"),
                Some(""),
            ])) as ArrayRef,
        ],
        None,
    );
    let map: ArrayRef = Arc::new(MapArray::new(
        Arc::new(Field::new("entries", DataType::Struct(fields), false)),
        OffsetBuffer::new(vec![0i32, 2, 2, 3, 4].into()),
        entries,
        Some(NullBuffer::from(vec![true, false, true, true])),
        false,
    ));
    for value in nested().into_iter().chain([map]) {
        for name in ["array_agg", "array_agg_distinct"] {
            check(
                AggregateDiffSpec::new(name)
                    .original_state_interpretation(
                        false,
                        vec![K {
                            ascending: false,
                            nulls_first: true,
                        }],
                    )
                    .column(value.clone())
                    .column(Arc::new(Int64Array::from(vec![
                        Some(3),
                        None,
                        Some(2),
                        Some(1),
                    ])))
                    .grouped(vec![0, 0, 0, 0], 2)
                    .partitions(3, 2409),
            );
        }
    }
}

#[test]
fn pure_differential_array_logical_largeint_keeps_original_signature_drift_golden() {
    for name in ["array_agg", "array_agg_distinct"] {
        for nullable in [false, true] {
            for rows in [0, 8] {
                let source = FunctionValueType::try_with_logical_type(
                    DataType::FixedSizeBinary(16),
                    nullable,
                    ValueLogicalType::LargeInt,
                )
                .unwrap();
                let failure = run_aggregate_differential(
                    &AggregateDiffSpec::new(name)
                        .original_state_interpretation(false, vec![])
                        .typed_column(source.clone(), values(&source, rows)),
                )
                .unwrap_err();
                let DifferentialFailure::LegacyUnavailable {
                    name: actual,
                    reason,
                } = failure
                else {
                    panic!("expected original ARRAY logical LARGEINT signature drift: {failure}")
                };
                assert_eq!(actual, name);
                let state = "List(Field { data_type: FixedSizeBinary(16), nullable: true })";
                let result = "List(Field { data_type: FixedSizeBinary(16), nullable: true, metadata: {\"nr_logical_type\": \"largeint\"} })";
                let expected = format!(
                    "bind aggregate `{name}`: aggregate `{name}` resolved signature drift: planned=ResolvedAggregateSignature {{ overload: AggregateOverloadIdentity(\"builtin.aggregate/{name}/derived-v1\"), argument_types: [FixedSizeBinary(16)], intermediate_type: {state}, output_type: {result}, state_format: AggregateStateFormatId(\"novarocks/{name}/state-v1\") }}, local=ResolvedAggregateSignature {{ overload: AggregateOverloadIdentity(\"builtin.aggregate/{name}/derived-v1\"), argument_types: [FixedSizeBinary(16)], intermediate_type: {state}, output_type: {state}, state_format: AggregateStateFormatId(\"novarocks/{name}/state-v1\") }}"
                );
                assert_eq!(reason, expected);
            }
        }
    }
}
