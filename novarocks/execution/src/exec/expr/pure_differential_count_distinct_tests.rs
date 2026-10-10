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

//! COUNT DISTINCT exact profiles, tuple NULL rules and grouped four phases.
use super::aggregate::{AggregateDiffSpec, assert_aggregate_matches_v1};
use arrow::array::*;
use arrow::datatypes::{DataType, Field, Fields, Int32Type, TimeUnit};
use arrow_buffer::i256;
use novarocks_type_contract::FunctionValueType;
use std::sync::Arc;
fn check(values: ArrayRef, nullable: bool) {
    let rows = values.len();
    let summary = assert_aggregate_matches_v1(
        AggregateDiffSpec::new("multi_distinct_count")
            .typed_column(
                FunctionValueType::new(values.data_type().clone(), nullable),
                values,
            )
            .grouped((0..rows).map(|row| row % 3).collect(), 5)
            .partitions(7, 0xc001),
    );
    assert_eq!(summary.matched_failures, 0);
    assert_eq!(summary.null_results, 0);
    assert_eq!(summary.result_type.data_type, DataType::Int64);
    assert!(!summary.result_type.nullable);
    assert_eq!(summary.pure_state_type.data_type, DataType::Binary);
}
#[test]
fn pure_differential_count_distinct_all_raw_flat_profiles_and_zero_groups() {
    for nullable in [false, true] {
        let pattern = (0..516)
            .map(|row| {
                if nullable && row % 11 == 0 {
                    None
                } else {
                    Some((row % 7) as i64 - 3)
                }
            })
            .collect::<Vec<_>>();
        let integers = Arc::new(Int64Array::from(pattern.clone())) as ArrayRef;
        for ty in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
        ] {
            check(arrow::compute::cast(&integers, &ty).unwrap(), nullable);
        }
        check(
            Arc::new(Date32Array::from(
                pattern
                    .iter()
                    .map(|v| v.map(|n| n as i32))
                    .collect::<Vec<_>>(),
            )),
            nullable,
        );
        check(
            Arc::new(BooleanArray::from(
                pattern.iter().map(|v| v.map(|n| n > 0)).collect::<Vec<_>>(),
            )),
            nullable,
        );
        let strings = pattern
            .iter()
            .map(|v| v.map(|n| if n > 0 { "a" } else { "" }))
            .collect::<Vec<_>>();
        check(Arc::new(StringArray::from(strings.clone())), nullable);
        check(
            Arc::new(BinaryArray::from(
                strings
                    .iter()
                    .map(|v| v.map(str::as_bytes))
                    .collect::<Vec<_>>(),
            )),
            nullable,
        );
        for (p, s) in [(9, 0), (18, 2), (38, 6)] {
            check(
                Arc::new(
                    Decimal128Array::from(
                        pattern
                            .iter()
                            .map(|v| v.map(|n| n as i128 * 1001))
                            .collect::<Vec<_>>(),
                    )
                    .with_precision_and_scale(p, s)
                    .unwrap(),
                ),
                nullable,
            );
        }
        for (p, s) in [(40, 0), (60, 2), (76, 6)] {
            check(
                Arc::new(
                    Decimal256Array::from(
                        pattern
                            .iter()
                            .map(|v| v.map(|n| i256::from_i128(n as i128 * 1001)))
                            .collect::<Vec<_>>(),
                    )
                    .with_precision_and_scale(p, s)
                    .unwrap(),
                ),
                nullable,
            );
        }
        for ty in [
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        ] {
            check(arrow::compute::cast(&integers, &ty).unwrap(), nullable);
        }
    }
    for input in [
        Arc::new(
            TimestampSecondArray::from(vec![Some(1), None, Some(1), Some(-1)])
                .with_timezone("Pacific/Apia"),
        ) as ArrayRef,
        Arc::new(
            TimestampMillisecondArray::from(vec![Some(1), None, Some(1), Some(-1)])
                .with_timezone("UTC"),
        ) as ArrayRef,
        Arc::new(
            TimestampMicrosecondArray::from(vec![Some(1), None, Some(1), Some(-1)])
                .with_timezone("Europe/Paris"),
        ) as ArrayRef,
        Arc::new(
            TimestampNanosecondArray::from(vec![Some(1), None, Some(1), Some(-1)])
                .with_timezone("Etc/UTC"),
        ) as ArrayRef,
    ] {
        check(input, true);
    }
    check(Arc::new(NullArray::new(516)), true);
    for ty in [DataType::Int64, DataType::Utf8, DataType::Decimal256(60, 3)] {
        check(new_empty_array(&ty), true);
        check(new_null_array(&ty, 513), true);
    }
}
#[test]
fn pure_differential_count_distinct_preserves_signed_zero_nan_bits_and_slice_addresses() {
    let patterns = [
        0.0,
        -0.0,
        f64::from_bits(0x7ff8_0000_0000_0011),
        f64::from_bits(0x7ff8_0000_0000_0012),
        f64::INFINITY,
        f64::NEG_INFINITY,
    ];
    let input = Arc::new(Float64Array::from(
        (0..520)
            .map(|r| {
                if r % 13 == 0 {
                    None
                } else {
                    Some(patterns[r % patterns.len()])
                }
            })
            .collect::<Vec<_>>(),
    )) as ArrayRef;
    check(input.slice(2, 516), true);
    let patterns = [
        0.0,
        -0.0,
        f32::from_bits(0x7fc0_0011),
        f32::from_bits(0x7fc0_0012),
        f32::INFINITY,
        f32::NEG_INFINITY,
    ];
    let input = Arc::new(Float32Array::from(
        (0..520)
            .map(|r| {
                if r % 13 == 0 {
                    None
                } else {
                    Some(patterns[r % patterns.len()])
                }
            })
            .collect::<Vec<_>>(),
    )) as ArrayRef;
    check(input.slice(2, 516), true);
}
#[test]
fn pure_differential_count_distinct_recursive_list_struct_null_and_map_child_profiles() {
    let list = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
        (0..516).map(|row| match row % 5 {
            0 => None,
            1 => Some(vec![]),
            2 => Some(vec![None]),
            3 => Some(vec![Some(1), None]),
            _ => Some(vec![Some(1), Some(2)]),
        }),
    )) as ArrayRef;
    check(list.clone(), true);
    let nested = Arc::new(StructArray::new(
        Fields::from(vec![Field::new("list", list.data_type().clone(), true)]),
        vec![list],
        None,
    )) as ArrayRef;
    check(nested.clone(), true);
    let outer = Arc::new(StructArray::new(
        Fields::from(vec![Field::new("nested", nested.data_type().clone(), true)]),
        vec![nested],
        None,
    )) as ArrayRef;
    check(outer, true);
    let large = Arc::new(LargeBinaryArray::from(
        (0..516)
            .map(|row| {
                if row % 4 == 0 {
                    None
                } else {
                    Some(&b"bytes"[..])
                }
            })
            .collect::<Vec<_>>(),
    )) as ArrayRef;
    let input = Arc::new(StructArray::new(
        Fields::from(vec![Field::new("binary", large.data_type().clone(), true)]),
        vec![large],
        None,
    )) as ArrayRef;
    check(input, true);
    let mut maps = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
    for row in 0..516 {
        if row % 5 == 0 {
            maps.append(false).unwrap();
        } else {
            maps.keys().append_value("k");
            if row % 2 == 0 {
                maps.values().append_null();
            } else {
                maps.values().append_value(1);
            }
            maps.append(true).unwrap();
        }
    }
    let maps = Arc::new(maps.finish()) as ArrayRef;
    let input = Arc::new(StructArray::new(
        Fields::from(vec![Field::new("map", maps.data_type().clone(), true)]),
        vec![maps],
        None,
    )) as ArrayRef;
    check(input, true);
    let mut fixed = FixedSizeBinaryBuilder::new(16);
    for row in 0..516 {
        if row % 7 == 0 {
            fixed.append_null();
        } else {
            fixed
                .append_value(((row % 3) as i128).to_le_bytes())
                .unwrap();
        }
    }
    let fixed = Arc::new(fixed.finish()) as ArrayRef;
    let input = Arc::new(StructArray::new(
        Fields::from(vec![Field::new(
            "largeint",
            DataType::FixedSizeBinary(16),
            true,
        )]),
        vec![fixed],
        None,
    )) as ArrayRef;
    check(input, true);
}
#[test]
fn pure_differential_count_distinct_variadic_preserves_packed_struct_null_predicate() {
    let ints = Arc::new(Int64Array::from(
        (0..516)
            .map(|row| {
                if row % 5 == 0 {
                    None
                } else {
                    Some((row % 7) as i64)
                }
            })
            .collect::<Vec<_>>(),
    )) as ArrayRef;
    let strs = Arc::new(StringArray::from(
        (0..516)
            .map(|row| {
                if row % 9 == 0 {
                    None
                } else {
                    Some(if row % 2 == 0 { "a" } else { "b" })
                }
            })
            .collect::<Vec<_>>(),
    )) as ArrayRef;
    let list = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
        (0..516).map(|row| {
            Some(if row % 2 == 0 {
                vec![None]
            } else {
                vec![Some(1)]
            })
        }),
    )) as ArrayRef;
    for arrays in [vec![ints.clone(), strs.clone()], vec![ints, strs, list]] {
        let mut spec = AggregateDiffSpec::new("multi_distinct_count")
            .grouped((0..516).map(|row| row % 4).collect(), 6)
            .partitions(7, 0xc002);
        for array in arrays {
            spec = spec.column(array);
        }
        let summary = assert_aggregate_matches_v1(spec);
        assert_eq!(summary.matched_failures, 0);
        assert_eq!(summary.null_results, 0);
    }
}
