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

//! Permanent exact whole Data checks; no row attribution or containment allowance.
use super::aggregate::{AggregateDiffSpec, assert_aggregate_matches_v1};
use arrow::array::{
    ArrayRef, Int64Array, StructArray, UInt32Array, new_empty_array, new_null_array,
};
use arrow::datatypes::{DataType, Field, Fields};
use std::sync::Arc;
#[test]
fn pure_differential_ndv_unsupported_full_domain_retains_empty_and_all_null_success() {
    for name in ["ndv", "approx_count_distinct"] {
        for data_type in [
            DataType::UInt32,
            DataType::Decimal256(76, 6),
            DataType::List(Arc::new(Field::new(
                "original_child",
                DataType::Int64,
                true,
            ))),
        ] {
            for values in [new_empty_array(&data_type), new_null_array(&data_type, 521)] {
                let summary =
                    assert_aggregate_matches_v1(AggregateDiffSpec::new(name).typed_column(
                        novarocks_type_contract::FunctionValueType::new(data_type.clone(), true),
                        values,
                    ));
                assert_eq!(summary.matched_failures, 0);
            }
        }
        let values: ArrayRef = Arc::new(UInt32Array::from(vec![None, Some(7), Some(8), None]));
        let summary = assert_aggregate_matches_v1(
            AggregateDiffSpec::new(name)
                .typed_column(
                    novarocks_type_contract::FunctionValueType::new(DataType::UInt32, true),
                    values,
                )
                .grouped(vec![0, 0, 1, 1], 3)
                .partitions(3, 0x4e4456),
        );
        assert_eq!(summary.matched_failures, 2);
    }
}
#[test]
fn pure_differential_ndv_actual_nested_metadata_error_is_byte_exact_beyond_row_bound() {
    let fields: Fields = (0..96)
        .map(|i| {
            Arc::new(
                Field::new(format!("raw_child_{i}"), DataType::Int64, true)
                    .with_metadata([(format!("provider_{i}"), format!("original_{i}"))].into()),
            )
        })
        .collect();
    let values: ArrayRef = Arc::new(StructArray::new(
        fields.clone(),
        fields
            .iter()
            .map(|_| Arc::new(Int64Array::from(vec![None, Some(7), Some(8)])) as ArrayRef)
            .collect(),
        Some(arrow::buffer::NullBuffer::from(vec![false, true, true])),
    ));
    for name in ["ndv", "approx_count_distinct"] {
        let summary = assert_aggregate_matches_v1(
            AggregateDiffSpec::new(name)
                .typed_column(
                    novarocks_type_contract::FunctionValueType::new(
                        values.data_type().clone(),
                        true,
                    ),
                    values.clone(),
                )
                .grouped(vec![0, 0, 1], 3)
                .partitions(3, 0x44415441),
        );
        assert_eq!(summary.matched_failures, 2);
    }
}
