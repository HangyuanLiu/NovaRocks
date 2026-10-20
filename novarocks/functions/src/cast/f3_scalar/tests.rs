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

//! Independent expected values for migrated scalar cast families.
use super::*;
use std::time::Duration;

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("CAST does not wait")
    }
}
fn recipe(
    source: DataType,
    target: DataType,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> PreparedCastRecipe {
    PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &FunctionValueType::new(source, true),
        &FunctionValueType::new(target, true),
        policy,
        allow,
        &Control,
    )
    .unwrap()
}
fn modes() -> [(DecimalOverflowPolicy, bool); 4] {
    [
        (DecimalOverflowPolicy::OutputNull, false),
        (DecimalOverflowPolicy::OutputNull, true),
        (DecimalOverflowPolicy::ReportError, false),
        (DecimalOverflowPolicy::ReportError, true),
    ]
}
fn check(input: ArrayRef, target: DataType, expected: &[CastRowResult]) {
    for (policy, allow) in modes() {
        let r = recipe(input.data_type().clone(), target.clone(), policy, allow);
        for (row, expected) in expected.iter().enumerate() {
            assert_eq!(
                &r.evaluate_row(EvaluatedArgument::Column(&input), row, row, &Control)
                    .unwrap(),
                expected
            );
        }
    }
}
#[test]
fn decimal_integer_truncates_toward_zero_and_retains_actual_epoch_days() {
    for target in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ] {
        let input = Arc::new(
            Decimal128Array::from(vec![Some(12799), Some(-12799), None])
                .with_precision_and_scale(8, 2)
                .unwrap(),
        ) as ArrayRef;
        check(
            input,
            target.clone(),
            &[
                CastRowResult::Signed(127),
                CastRowResult::Signed(-127),
                CastRowResult::Null,
            ],
        );
        let input = Arc::new(
            Decimal256Array::from(vec![
                Some(i256::from_i128(12799)),
                Some(i256::from_i128(-12799)),
                None,
            ])
            .with_precision_and_scale(76, 2)
            .unwrap(),
        ) as ArrayRef;
        check(
            input,
            target,
            &[
                CastRowResult::Signed(127),
                CastRowResult::Signed(-127),
                CastRowResult::Null,
            ],
        );
    }
    let input = Arc::new(Date32Array::from(vec![
        Some(0),
        Some(-1),
        Some(20000),
        None,
    ])) as ArrayRef;
    for target in [DataType::Int32, DataType::Int64] {
        check(
            Arc::clone(&input),
            target,
            &[
                CastRowResult::Signed(0),
                CastRowResult::Signed(-1),
                CastRowResult::Signed(20000),
                CastRowResult::Null,
            ],
        );
    }
    for target in [DataType::Int8, DataType::Int16] {
        assert_eq!(
            PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &FunctionValueType::new(DataType::Date32, true),
                &FunctionValueType::new(target, true),
                DecimalOverflowPolicy::OutputNull,
                false,
                &Control
            ),
            Err(CastPrepareError::Unsupported)
        );
    }
}
#[test]
fn decimal_integer_overflow_is_policy_bound_not_float_allow_bound() {
    for wide in [false, true] {
        let input: ArrayRef = if wide {
            Arc::new(
                Decimal256Array::from(vec![i256::from_i128(128), i256::from_i128(-129)])
                    .with_precision_and_scale(76, 0)
                    .unwrap(),
            )
        } else {
            Arc::new(
                Decimal128Array::from(vec![128, -129])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            )
        };
        for (policy, allow) in modes() {
            let r = recipe(input.data_type().clone(), DataType::Int8, policy, allow);
            for row in 0..2 {
                let out = r
                    .evaluate_row(EvaluatedArgument::Column(&input), row, row, &Control)
                    .unwrap();
                if policy == DecimalOverflowPolicy::ReportError {
                    assert!(matches!(out, CastRowResult::RowError(_)));
                } else {
                    assert_eq!(out, CastRowResult::Null);
                }
            }
        }
    }
    let input = Arc::new(
        Decimal128Array::from(vec![12, -12])
            .with_precision_and_scale(8, -2)
            .unwrap(),
    ) as ArrayRef;
    check(
        input,
        DataType::Int64,
        &[CastRowResult::Signed(1200), CastRowResult::Signed(-1200)],
    );
}
#[test]
fn text_parsers_preserve_invalid_null_and_decimal_rounding() {
    let input = Arc::new(StringArray::from(vec![
        Some("1.25"),
        Some("-2.5"),
        Some("bad"),
        Some("NaN"),
        Some("inf"),
        None,
    ])) as ArrayRef;
    check(
        Arc::clone(&input),
        DataType::Float32,
        &[
            CastRowResult::Float32(1.25),
            CastRowResult::Float32(-2.5),
            CastRowResult::Null,
            CastRowResult::Null,
            CastRowResult::Null,
            CastRowResult::Null,
        ],
    );
    check(
        input,
        DataType::Float64,
        &[
            CastRowResult::Float64(1.25),
            CastRowResult::Float64(-2.5),
            CastRowResult::Null,
            CastRowResult::Null,
            CastRowResult::Null,
            CastRowResult::Null,
        ],
    );
    let input = Arc::new(StringArray::from(vec![
        Some("1.25"),
        Some("-1.25"),
        Some(""),
        Some("  "),
        Some("bad"),
        None,
    ])) as ArrayRef;
    check(
        input,
        DataType::Decimal128(8, 1),
        &[
            CastRowResult::Decimal128(13),
            CastRowResult::Decimal128(-13),
            CastRowResult::Null,
            CastRowResult::Null,
            CastRowResult::Null,
            CastRowResult::Null,
        ],
    );
}
#[test]
fn float32_decimal_uses_the_existing_float_author() {
    let input = Arc::new(Float32Array::from(vec![
        Some(1.25),
        Some(-1.25),
        Some(f32::INFINITY),
        None,
    ])) as ArrayRef;
    check(
        input,
        DataType::Decimal128(8, 1),
        &[
            CastRowResult::Decimal128(13),
            CastRowResult::Decimal128(-13),
            CastRowResult::Null,
            CastRowResult::Null,
        ],
    );
}
#[test]
fn decimal256_narrows_with_declared_precision_and_scale() {
    let input = Arc::new(
        Decimal256Array::from(vec![
            Some(i256::from_i128(125)),
            Some(i256::from_i128(-125)),
            None,
        ])
        .with_precision_and_scale(76, 2)
        .unwrap(),
    ) as ArrayRef;
    check(
        input,
        DataType::Decimal128(8, 1),
        &[
            CastRowResult::Decimal128(13),
            CastRowResult::Decimal128(-13),
            CastRowResult::Null,
        ],
    );
    let input = Arc::new(
        Decimal256Array::from(vec![i256::from_i128(1000)])
            .with_precision_and_scale(76, 0)
            .unwrap(),
    ) as ArrayRef;
    for (policy, allow) in modes() {
        let r = recipe(
            input.data_type().clone(),
            DataType::Decimal128(3, 0),
            policy,
            allow,
        );
        let out = r
            .evaluate_row(EvaluatedArgument::Column(&input), 0, 0, &Control)
            .unwrap();
        if policy == DecimalOverflowPolicy::ReportError || allow {
            assert!(matches!(out, CastRowResult::RowError(_)));
        } else {
            assert_eq!(out, CastRowResult::Null);
        }
    }
}
#[test]
fn boolean_decimal_and_decimal256_truth_value_are_independent_of_scale() {
    let input = Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])) as ArrayRef;
    check(
        Arc::clone(&input),
        DataType::Decimal128(8, 2),
        &[
            CastRowResult::Decimal128(100),
            CastRowResult::Decimal128(0),
            CastRowResult::Null,
        ],
    );
    check(
        input,
        DataType::Decimal128(8, -2),
        &[
            CastRowResult::Decimal128(0),
            CastRowResult::Decimal128(0),
            CastRowResult::Null,
        ],
    );
    let input = Arc::new(
        Decimal256Array::from(vec![
            Some(i256::from_i128(1)),
            Some(i256::ZERO),
            Some(i256::from_i128(-1)),
            None,
        ])
        .with_precision_and_scale(76, 20)
        .unwrap(),
    ) as ArrayRef;
    check(
        input,
        DataType::Boolean,
        &[
            CastRowResult::Boolean(true),
            CastRowResult::Boolean(false),
            CastRowResult::Boolean(true),
            CastRowResult::Null,
        ],
    );
}
#[test]
fn extended_recipes_do_not_authorize_nominal_domain_changes() {
    let source =
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap();
    let target = FunctionValueType::new(DataType::Decimal128(8, 1), true);
    assert_eq!(
        PreparedCastRecipe::try_new(
            CastOperation::Carrier,
            &source,
            &target,
            DecimalOverflowPolicy::OutputNull,
            false,
            &Control
        ),
        Err(CastPrepareError::Unsupported)
    );
}

#[test]
fn decimal_calendar_reuses_numeric_calendar_grammar() {
    let input = Arc::new(
        Decimal128Array::from(vec![Some(202001019), Some(999999999), None])
            .with_precision_and_scale(18, 1)
            .unwrap(),
    ) as ArrayRef;
    check(
        input,
        DataType::Date32,
        &[
            CastRowResult::Signed(18262),
            CastRowResult::Null,
            CastRowResult::Null,
        ],
    );
}
