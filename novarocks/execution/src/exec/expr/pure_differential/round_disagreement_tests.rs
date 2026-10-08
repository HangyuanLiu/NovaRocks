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
//! Independent regression evidence for known ROUND v1/owner disagreement.
//! These tests pin the gap; passing them does not claim differential equality.
use super::{
    DifferentialFailure, ScalarDiffSpec, assert_scalar_matches_v1, run_scalar_differential,
};
use arrow::array::{ArrayRef, Decimal128Array, Decimal256Array, Float64Array, Int64Array};
use novarocks_type_contract::DecimalOverflowPolicy;
use std::sync::Arc;
fn signed(values: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}
fn floats(values: Vec<Option<f64>>) -> ArrayRef {
    Arc::new(Float64Array::from(values))
}
fn decimal(values: Vec<Option<i128>>, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values)
            .with_precision_and_scale(38, scale)
            .unwrap(),
    )
}
fn mismatch(spec: ScalarDiffSpec, required: &str) {
    match run_scalar_differential(&spec) {
        Err(DifferentialFailure::Mismatch {
            name,
            overload,
            details,
        }) => {
            assert_eq!(name, "round");
            assert_eq!(overload.as_str(), "builtin.scalar/round/dynamic-v1");
            let text = details.join("\n");
            assert!(
                text.contains(required),
                "expected {required:?}, actual {text}"
            );
        }
        result => panic!("expected original ROUND disagreement, actual {result:?}"),
    }
}
#[test]
fn original_round_minimum_runtime_digits_gap_and_null_mask_are_frozen() {
    let spec = ScalarDiffSpec::new("round")
        .column(floats(vec![Some(1.25)]))
        .column(signed(vec![Some(i64::MIN)]));
    if cfg!(debug_assertions) {
        mismatch(spec, "attempt to negate with overflow");
    } else {
        assert_scalar_matches_v1(spec);
    }
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("round")
            .column(floats(vec![None]))
            .column(signed(vec![Some(i64::MIN)])),
    );
}
#[test]
fn original_round_valid_precision38_input_can_produce_legacy_raw_out_of_precision() {
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        mismatch(
            ScalarDiffSpec::new("round")
                .column(decimal(
                    vec![
                        Some(10_i128.pow(38) - 1),
                        Some(-(10_i128.pow(38) - 1)),
                        None,
                    ],
                    0,
                ))
                .column(signed(vec![Some(-1); 3]))
                .decimal_overflow(policy),
            if policy == DecimalOverflowPolicy::OutputNull {
                "NULL"
            } else {
                "pure raised"
            },
        );
    }
}
#[test]
fn original_round_wide_decimal_digits_arrow_safe_null_but_owner_report_error_gap() {
    let d128 = decimal(vec![Some(10000000000000000000)], 0);
    let d256: ArrayRef = Arc::new(
        Decimal256Array::from(vec![Some(arrow_buffer::i256::from_i128(
            10000000000000000000,
        ))])
        .with_precision_and_scale(76, 0)
        .unwrap(),
    );
    for digits in [d128, d256] {
        let spec = ScalarDiffSpec::new("round")
            .column(floats(vec![Some(1.25)]))
            .column(digits);
        assert_scalar_matches_v1(
            spec.clone()
                .decimal_overflow(DecimalOverflowPolicy::OutputNull),
        );
        mismatch(
            spec.decimal_overflow(DecimalOverflowPolicy::ReportError),
            "decimal overflow in round digits",
        );
    }
}
#[test]
fn original_round_required_decimal_failure_and_error_text_gap_are_frozen() {
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        let spec = ScalarDiffSpec::new("round")
            .column(decimal(vec![Some(10_i128.pow(38) - 1)], 0))
            .column(signed(vec![Some(1)]))
            .decimal_overflow(policy);
        mismatch(
            spec,
            if policy == DecimalOverflowPolicy::OutputNull {
                "required error swallowed"
            } else {
                "does not contain pure row error"
            },
        );
        let spec = ScalarDiffSpec::new("round")
            .column(decimal(vec![Some(1)], 38))
            .column(signed(vec![Some(-38)]))
            .decimal_overflow(policy);
        mismatch(
            spec,
            if policy == DecimalOverflowPolicy::OutputNull {
                "required error swallowed"
            } else {
                "does not contain pure row error"
            },
        );
    }
}
