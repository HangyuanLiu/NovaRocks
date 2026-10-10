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

//! Permanent full accurate NDV input-carrier differential through original aggregate harness.
use super::aggregate::{AggregateDiffSpec, assert_aggregate_matches_v1};
use super::{HarnessControl, constant_policy};
use arrow::array::{
    Array, ArrayRef, Float64Array, Int32Array, StringArray, new_empty_array, new_null_array,
};
use arrow::datatypes::{DataType, Field, TimeUnit};
use novarocks_functions::ConstantPool;
use novarocks_type_contract::{CompilePhase, FunctionValueType};
use std::sync::Arc;
const NAMES: [&str; 2] = ["ndv", "approx_count_distinct"];
fn profile(name: &str, values: ArrayRef, nullable: bool, seed: u64) {
    let mapping = (0..values.len()).map(|row| row % 5).collect::<Vec<_>>();
    let summary = assert_aggregate_matches_v1(
        AggregateDiffSpec::new(name)
            .typed_column(
                FunctionValueType::new(values.data_type().clone(), nullable),
                values,
            )
            .grouped(mapping, 7)
            .partitions(7, seed),
    );
    assert_eq!(summary.matched_failures, 0);
    assert_eq!(summary.result_type.data_type, DataType::Int64);
    assert_eq!(summary.pure_state_type.data_type, DataType::Binary);
    assert_eq!(summary.legacy_intermediate_type, DataType::Binary);
    assert_eq!(summary.null_results, 0);
}
#[test]
fn pure_differential_ndv_hll_every_numeric_date_timestamp_hash_carrier() {
    let integers: ArrayRef = Arc::new(Int32Array::from(
        (0..521)
            .map(|row| {
                if row % 11 == 0 {
                    None
                } else {
                    Some((row % 9) as i32 - 4)
                }
            })
            .collect::<Vec<_>>(),
    ));
    for name in NAMES {
        for nullable in [true, false] {
            let source = if nullable {
                integers.clone()
            } else {
                Arc::new(Int32Array::from(
                    (0..521).map(|row| (row % 9) as i32 - 4).collect::<Vec<_>>(),
                )) as ArrayRef
            };
            for (ordinal, ty) in [
                DataType::Int8,
                DataType::Int16,
                DataType::Int32,
                DataType::Int64,
                DataType::Float32,
                DataType::Float64,
                DataType::Date32,
                DataType::Timestamp(TimeUnit::Second, None),
                DataType::Timestamp(TimeUnit::Millisecond, None),
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                DataType::Decimal128(38, 0),
            ]
            .into_iter()
            .enumerate()
            {
                let original = if matches!(ty, DataType::Timestamp(..)) {
                    arrow::compute::cast(&source, &DataType::Int64).unwrap()
                } else {
                    source.clone()
                };
                let values = arrow::compute::cast(&original, &ty).unwrap();
                profile(name, values, nullable, 0x484c + ordinal as u64);
            }
        }
    }
}
#[test]
fn pure_differential_ndv_hll_string_binary_bool_and_fixed_bytes_profiles() {
    let strings: ArrayRef = Arc::new(StringArray::from(
        (0..521)
            .map(|row| {
                if row % 11 == 0 {
                    None
                } else {
                    Some(format!("value{}", row % 9))
                }
            })
            .collect::<Vec<_>>(),
    ));
    for name in NAMES {
        for ty in [
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Binary,
            DataType::LargeBinary,
        ] {
            profile(
                name,
                arrow::compute::cast(&strings, &ty).unwrap(),
                true,
                0x5655,
            );
        }
        let boolean: ArrayRef = Arc::new(arrow::array::BooleanArray::from(
            (0..521)
                .map(|row| (row % 11 != 0).then_some(row % 2 == 0))
                .collect::<Vec<_>>(),
        ));
        profile(name, boolean, true, 0x5656);
        let mut fixed = arrow::array::FixedSizeBinaryBuilder::new(16);
        for row in 0..521 {
            if row % 11 == 0 {
                fixed.append_null()
            } else {
                fixed.append_value((row as i128 % 9).to_le_bytes()).unwrap();
            }
        }
        profile(name, Arc::new(fixed.finish()), true, 0x5657);
    }
}
#[test]
fn pure_differential_ndv_hll_float_nan_zero_infinity_exact_and_empty_null_groups() {
    let pattern = [
        Some(0.0),
        Some(-0.0),
        Some(f64::from_bits(0x7ff8000000000001)),
        Some(f64::from_bits(0xfff8000000001234)),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        None,
    ];
    let floats: ArrayRef = Arc::new(Float64Array::from(
        (0..521)
            .map(|row| pattern[row % pattern.len()])
            .collect::<Vec<_>>(),
    ));
    for name in NAMES {
        profile(name, floats.clone(), true, 0x4444);
        for ty in [
            DataType::Int32,
            DataType::Float64,
            DataType::Utf8,
            DataType::Binary,
        ] {
            for values in [new_empty_array(&ty), new_null_array(&ty, 17)] {
                profile(name, values, true, 0x4545);
            }
        }
    }
}
#[test]
fn pure_differential_ndv_hll_original_constant_ordinal_broadcast() {
    let ty = FunctionValueType::new(DataType::Utf8, true);
    let values = StringArray::from(vec![None, Some("chosen"), Some("excluded")]);
    let pool = ConstantPool::try_new(
        Arc::new(Field::new("original", DataType::Utf8, true)),
        ty,
        values.to_data(),
        constant_policy(),
        CompilePhase::Validate,
        &HarnessControl,
    )
    .unwrap();
    for name in NAMES {
        for ordinal in [0, 1] {
            let summary = assert_aggregate_matches_v1(
                AggregateDiffSpec::new(name)
                    .constant(pool.value(ordinal).unwrap())
                    .constant_rows(521)
                    .grouped((0..521).map(|r| r % 5).collect(), 7)
                    .partitions(7, 0x5757),
            );
            assert_eq!(summary.matched_failures, 0);
            assert_eq!(summary.null_results, 0);
        }
    }
}
