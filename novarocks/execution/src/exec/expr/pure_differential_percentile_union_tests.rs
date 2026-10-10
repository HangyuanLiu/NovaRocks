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

//! Permanent complete declared ANY1 probes; raw errors remain original whole Data.
use super::aggregate::{AggregateDiffSpec, assert_aggregate_matches_v1};
use arrow::array::{
    ArrayRef, BinaryArray, Float64Array, LargeBinaryArray, LargeStringArray, StringArray,
    StructArray, new_null_array,
};
use arrow::datatypes::{DataType, Field, Fields, IntervalUnit, TimeUnit};
use std::sync::Arc;
fn check(input: ArrayRef) {
    let rows = input.len();
    assert_aggregate_matches_v1(
        AggregateDiffSpec::new("percentile_union")
            .typed_column(
                novarocks_type_contract::FunctionValueType::new(input.data_type().clone(), true),
                input,
            )
            .grouped((0..rows).map(|i| i % 3).collect(), 4)
            .partitions(3, 762109),
    );
}
#[test]
fn pure_differential_percentile_union_binary_phases_null_slice_and_distinct() {
    let values = (0..321)
        .map(|i| {
            if i % 11 == 0 {
                None
            } else {
                Some(
                    novarocks_functions::approx_percentile_core::encode_single_value(
                        (i % 17) as f64,
                    ),
                )
            }
        })
        .collect::<Vec<_>>();
    let input = Arc::new(BinaryArray::from_iter(values.iter().map(|v| v.as_deref()))) as ArrayRef;
    check(input.clone());
    check(input.slice(7, 299));
    check(input.slice(0, 0));
    assert_aggregate_matches_v1(
        AggregateDiffSpec::new("percentile_union")
            .column(input)
            .original_state_interpretation(true, vec![])
            .partitions(3, 21719),
    );
}
#[test]
fn pure_differential_percentile_union_all_payload_carriers_and_original_invalid_formats() {
    let good = novarocks_functions::approx_percentile_core::encode_empty_state();
    let mut old = good.clone();
    old[1] = 3;
    for bytes in [good, old, vec![0xA2, 4], Vec::new(), b"bad".to_vec()] {
        check(Arc::new(BinaryArray::from(vec![
            Some(bytes.as_slice()),
            None,
            Some(bytes.as_slice()),
        ])));
        check(Arc::new(LargeBinaryArray::from(vec![
            Some(bytes.as_slice()),
            None,
            Some(bytes.as_slice()),
        ])));
    }
    let mut ascii = novarocks_functions::approx_percentile_core::encode_empty_state();
    ascii[0] = b'P';
    let ascii = String::from_utf8(ascii).unwrap();
    for text in [ascii.as_str(), "", "bad"] {
        check(Arc::new(StringArray::from(vec![
            Some(text),
            None,
            Some(text),
        ])));
        check(Arc::new(LargeStringArray::from(vec![
            Some(text),
            None,
            Some(text),
        ])));
    }
}
#[test]
fn pure_differential_percentile_union_original_struct_value_rate_compression_and_metadata() {
    for size in [0, 1, 2, 3, 5] {
        for length in [0, 9] {
            let columns = (0..size)
                .map(|i| {
                    Arc::new(Float64Array::from(vec![
                        if i == 1 {
                            Some(0.5)
                        } else if i == 2 {
                            Some(2048.)
                        } else {
                            Some(3.)
                        };
                        length
                    ])) as ArrayRef
                })
                .collect::<Vec<_>>();
            if size == 0 {
                check(Arc::new(StructArray::new_empty_fields(length, None)));
                continue;
            }
            let fields = columns
                .iter()
                .enumerate()
                .map(|(i, a)| {
                    Field::new(format!("original-input-{i}"), a.data_type().clone(), true)
                        .with_metadata([("provider-source".into(), format!("ordinal-{i}"))].into())
                })
                .collect::<Vec<_>>();
            check(Arc::new(StructArray::new(
                Fields::from(fields),
                columns,
                None,
            )));
        }
    }
}
#[test]
fn pure_differential_percentile_union_unsupported_any_carriers_null_empty_full_metadata() {
    let mut types = vec![
        DataType::Null,
        DataType::Boolean,
        DataType::UInt32,
        DataType::Int64,
        DataType::Float64,
        DataType::FixedSizeBinary(16),
        DataType::Decimal128(38, 2),
        DataType::Decimal256(76, 2),
        DataType::Date32,
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        DataType::Interval(IntervalUnit::MonthDayNano),
        DataType::List(Arc::new(
            Field::new("full-child", DataType::Utf8, true)
                .with_metadata([("source".into(), "retained".into())].into()),
        )),
    ];
    types.push(DataType::Struct(Fields::from(vec![
        Field::new(
            "long-child",
            DataType::Struct(
                (0..80)
                    .map(|i| Field::new(format!("original-long-{i}"), DataType::Utf8, true))
                    .collect(),
            ),
            true,
        ),
        Field::new("rate", DataType::Float64, true),
    ])));
    for ty in types {
        check(new_null_array(&ty, 3));
        check(new_null_array(&ty, 0));
    }
}
#[test]
fn pure_differential_percentile_union_actual_constant_pool_ordinal_and_broadcast() {
    let bytes = novarocks_functions::approx_percentile_core::encode_single_value(3.);
    let value = super::constant(
        novarocks_type_contract::FunctionValueType::new(DataType::Binary, false),
        Arc::new(BinaryArray::from(vec![bytes.as_slice()])),
    );
    assert_aggregate_matches_v1(
        AggregateDiffSpec::new("percentile_union")
            .constant(value)
            .constant_rows(321)
            .partitions(3, 7163),
    );
}

#[test]
fn pure_differential_percentile_union_nonzero_actual_constant_ordinal() {
    use arrow::array::Array;
    let bytes = novarocks_functions::approx_percentile_core::encode_single_value(3.);
    let values: ArrayRef = Arc::new(BinaryArray::from(vec![None, Some(bytes.as_slice()), None]));
    let ty = novarocks_type_contract::FunctionValueType::new(DataType::Binary, true);
    let pool = novarocks_functions::ConstantPool::try_new(
        Arc::new(ty.try_to_field("original-payload").unwrap()),
        ty,
        values.to_data(),
        super::constant_policy(),
        novarocks_type_contract::CompilePhase::FunctionSpecialization,
        &super::HarnessControl,
    )
    .unwrap();
    assert_aggregate_matches_v1(
        AggregateDiffSpec::new("percentile_union")
            .constant(pool.value(1).unwrap())
            .constant_rows(321)
            .partitions(3, 86213),
    );
}
