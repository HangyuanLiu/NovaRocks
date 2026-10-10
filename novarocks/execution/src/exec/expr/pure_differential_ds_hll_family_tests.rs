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

//! Permanent equality cases: all original DS HLL aggregate and scalar state records.
use super::aggregate::{AggregateDiffSpec, assert_aggregate_matches_v1};
use super::{LegacyConstantForm, ScalarDiffSpec, assert_scalar_matches_v1, constant};
use crate::exec::hll::{HllHandle, HllTargetType};
use arrow::array::{
    ArrayRef, BinaryArray, Float64Array, Int64Array, StringArray, UInt32Array, new_empty_array,
    new_null_array,
};
use arrow::datatypes::DataType;
use novarocks_type_contract::FunctionValueType;
use std::sync::Arc;
fn values() -> ArrayRef {
    Arc::new(StringArray::from(
        (0..73)
            .map(|r| {
                if r % 11 == 0 {
                    None
                } else {
                    Some(format!("value{}", r % 7))
                }
            })
            .collect::<Vec<_>>(),
    ))
}
fn aggregate(name: &str, arity: usize) {
    let mut spec = AggregateDiffSpec::new(name).column(values());
    if arity >= 2 {
        spec = spec.column(Arc::new(Int64Array::from(vec![10; 73])));
    }
    if arity >= 3 {
        spec = spec.column(Arc::new(StringArray::from(vec!["HLL_8"; 73])));
    }
    let summary = assert_aggregate_matches_v1(
        spec.grouped((0..73).map(|r| r % 3).collect(), 5)
            .partitions(5, 9100 + arity as u64),
    );
    assert_eq!(summary.matched_failures, 0);
    assert_eq!(summary.result_type.data_type, DataType::Int64);
    assert_eq!(summary.pure_state_type.data_type, DataType::Binary);
    assert_eq!(summary.null_results, 0);
}
#[test]
fn pure_differential_ds_hll_count_arity_one() {
    aggregate("ds_hll_count_distinct", 1);
}
#[test]
fn pure_differential_ds_hll_count_arity_two() {
    aggregate("ds_hll_count_distinct", 2);
}
#[test]
fn pure_differential_ds_hll_count_arity_three() {
    aggregate("ds_hll_count_distinct", 3);
}
#[test]
fn pure_differential_ds_hll_approx_arity_one() {
    aggregate("approx_count_distinct_hll_sketch", 1);
}
#[test]
fn pure_differential_ds_hll_approx_arity_two() {
    aggregate("approx_count_distinct_hll_sketch", 2);
}
#[test]
fn pure_differential_ds_hll_approx_arity_three() {
    aggregate("approx_count_distinct_hll_sketch", 3);
}
fn payloads() -> ArrayRef {
    let bytes = (0..13)
        .map(|r| {
            if r % 5 == 0 {
                None
            } else {
                let mut handle = HllHandle::new_unreserved(10, HllTargetType::Hll8).unwrap();
                handle.update_hash_unreserved(r % 3 + 1).unwrap();
                Some(handle.serialize().unwrap())
            }
        })
        .collect::<Vec<_>>();
    Arc::new(BinaryArray::from(
        bytes.iter().map(|a| a.as_deref()).collect::<Vec<_>>(),
    ))
}
#[test]
fn pure_differential_ds_hll_merge_payload_single_and_final() {
    let out = assert_aggregate_matches_v1(
        AggregateDiffSpec::new("ds_hll_count_distinct_merge")
            .column(payloads())
            .grouped((0..13).map(|r| r % 3).collect(), 5)
            .partitions(5, 9108),
    );
    assert_eq!(out.matched_failures, 0);
    assert_eq!(out.result_type.data_type, DataType::Int64);
    assert_eq!(out.null_results, 0);
}
#[test]
fn pure_differential_ds_hll_union_payload_single_and_final() {
    let out = assert_aggregate_matches_v1(
        AggregateDiffSpec::new("ds_hll_count_distinct_union")
            .column(payloads())
            .grouped((0..13).map(|r| r % 3).collect(), 5)
            .partitions(5, 9109),
    );
    assert_eq!(out.matched_failures, 0);
    assert_eq!(out.result_type.data_type, DataType::Binary);
    assert_eq!(out.null_results, 0);
}
fn state(arity: usize) {
    let mut spec = ScalarDiffSpec::new("ds_hll_count_distinct_state").column(values());
    if arity >= 2 {
        spec = spec.column(Arc::new(Int64Array::from(vec![10; 73])));
    }
    if arity >= 3 {
        spec = spec.column(Arc::new(StringArray::from(vec!["HLL_8"; 73])));
    }
    assert_scalar_matches_v1(spec.sparse_selections(7, 9200 + arity as u64));
}
#[test]
fn pure_differential_ds_hll_state_arity_one() {
    state(1);
}
#[test]
fn pure_differential_ds_hll_state_arity_two() {
    state(2);
}
#[test]
fn pure_differential_ds_hll_state_arity_three() {
    state(3);
}
#[test]
fn pure_differential_ds_hll_state_null_tuning_float_cast_and_true_errors() {
    let values = Arc::new(Int64Array::from(vec![
        None,
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        Some(5),
    ])) as ArrayRef;
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("ds_hll_count_distinct_state")
            .column(values.clone())
            .column(Arc::new(Int64Array::from(vec![-1, 10, 0, 266, 21, 4])))
            .column(Arc::new(StringArray::from(vec!["HLL_8"; 6])))
            .sparse_selections(7, 9210),
    );
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("ds_hll_count_distinct_state")
            .column(values)
            .column(Arc::new(Float64Array::from(vec![
                f64::NAN,
                10.9,
                0.,
                f64::INFINITY,
                21.9,
                4.1,
            ])))
            .column(Arc::new(StringArray::from(vec![
                "unknown", "hll_4", "HLL_6", "HLL_8", "", "HLL_4",
            ])))
            .sparse_selections(7, 9211),
    );
}
#[test]
fn pure_differential_ds_hll_state_empty_unsupported_and_all_null_domains() {
    for ty in [
        DataType::Utf8,
        DataType::Int64,
        DataType::Binary,
        DataType::UInt32,
        DataType::Null,
    ] {
        for values in [new_empty_array(&ty), new_null_array(&ty, 7)] {
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("ds_hll_count_distinct_state").column(values),
            );
        }
    }
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("ds_hll_count_distinct_state")
            .column(Arc::new(UInt32Array::from(vec![1, 2]))),
    );
}
#[test]
fn pure_differential_ds_hll_state_original_constant_source_broadcast() {
    let ty = FunctionValueType::new(DataType::Utf8, true);
    for scalar in [
        Arc::new(StringArray::from(vec![Some("chosen")])) as ArrayRef,
        Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
    ] {
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("ds_hll_count_distinct_state")
                .constant(constant(ty.clone(), scalar))
                .constant_rows(17)
                .legacy_constants(LegacyConstantForm::Pool)
                .sparse_selections(5, 9213),
        );
    }
}
