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

//! Permanent entire admitted Decimal128/256 Float32 profiles against actual original arena CAST.
use super::legacy_decimal_float32_cast_baseline_tests::{actual, input};
use arrow::array::{Array, ArrayRef, Decimal256Array, Float32Array};
use arrow::datatypes::DataType;
use arrow_buffer::{NullBuffer, i256};
use novarocks_functions::{
    CastOperation, CastRowResult, EvaluatedArgument, KernelEvaluationControl, KernelFailure,
    PreparedCastRecipe, SelectedValues, Selection,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, DecimalOverflowPolicy, FunctionValueType, PureCompileControl,
};
use std::{sync::Arc, time::Duration};
struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("decimal float cast never waits")
    }
}
fn r(
    a: &ArrayRef,
    nullable: bool,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> PreparedCastRecipe {
    PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &FunctionValueType::new(a.data_type().clone(), nullable),
        &FunctionValueType::new(
            DataType::Float32,
            nullable || matches!(a.data_type(), DataType::Decimal128(_, s) if *s < 0),
        ),
        policy,
        allow,
        &Control,
    )
    .unwrap()
}
fn value(
    r: &PreparedCastRecipe,
    a: &ArrayRef,
    ordinal: usize,
    row: usize,
    policy: DecimalOverflowPolicy,
    allow: bool,
) {
    let old = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        actual(&a.slice(row, 1), policy, allow).unwrap()
    }));
    let pure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        r.evaluate_row(EvaluatedArgument::Column(a), ordinal, row, &Control)
            .unwrap()
    }));
    assert_eq!(
        old.is_err(),
        pure.is_err(),
        "retain original panic on exactly demanded rows"
    );
    if let (Ok(old), Ok(pure)) = (old, pure) {
        let old = old.as_any().downcast_ref::<Float32Array>().unwrap();
        if old.is_null(0) {
            assert_eq!(pure, CastRowResult::Null);
        } else {
            let CastRowResult::Float32(got) = pure else {
                panic!("exact Float32 output")
            };
            assert_eq!(got.to_bits(), old.value(0).to_bits());
        }
    }
}
fn complete(wide: bool) {
    let max = if wide { 76 } else { 38 };
    // False->true result widening is separately checked through the same exact profile below.
    for p in 1..=max {
        for scale in i8::MIN..=p as i8 {
            for nullable in [false, true] {
                let a = input(
                    wide,
                    p,
                    scale,
                    if nullable {
                        vec![Some(1), Some(-1), None, Some(i128::MIN), Some(i128::MAX)]
                    } else {
                        vec![Some(1), Some(-1), Some(0), Some(i128::MIN), Some(i128::MAX)]
                    },
                );
                for policy in [
                    DecimalOverflowPolicy::OutputNull,
                    DecimalOverflowPolicy::ReportError,
                ] {
                    for allow in [false, true] {
                        let recipe = r(&a, nullable, policy, allow);
                        if wide && scale == i8::MIN {
                            // All metadata/policies remain admitted, with original non-NULL failure pinned for boundary precisions below.
                            if nullable {
                                value(&recipe, &a, 7, 2, policy, allow);
                            }
                            continue;
                        }
                        for row in 0..a.len() {
                            value(&recipe, &a, row, row, policy, allow);
                        }
                    }
                }
            }
        }
    }
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for allow in [false, true] {
            let a = input(wide, max, 2, vec![Some(0), Some(1), None, Some(-1)]).slice(1, 3);
            let recipe = r(&a, true, policy, allow);
            for (ordinal, row) in Selection::try_sparse(3, &[0, 2])
                .unwrap()
                .iter()
                .enumerate()
            {
                value(&recipe, &a, ordinal, row, policy, allow);
            }
            let compact = SelectedValues::try_new(
                Selection::try_sparse(3, &[0, 2]).unwrap(),
                a.data_type(),
                input(wide, max, 2, vec![Some(1), Some(-1)]),
                Box::default(),
            )
            .unwrap();
            for (ordinal, row) in [(0, 0), (1, 2)] {
                let expected = recipe
                    .evaluate_row(EvaluatedArgument::Column(&a), ordinal, row, &Control)
                    .unwrap();
                assert_eq!(
                    recipe
                        .evaluate_row(
                            EvaluatedArgument::SelectedColumn(&compact),
                            ordinal,
                            row,
                            &Control
                        )
                        .unwrap(),
                    expected
                );
            }
            let scalar = a.slice(2, 1);
            assert_eq!(
                recipe
                    .evaluate_row(EvaluatedArgument::Scalar(&scalar), 99, 500, &Control)
                    .unwrap(),
                recipe
                    .evaluate_row(EvaluatedArgument::Column(&a), 0, 2, &Control)
                    .unwrap()
            );
        }
    }
    let a = input(wide, max, 2, vec![Some(12345)]);
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for allow in [false, true] {
            let recipe = PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &FunctionValueType::new(a.data_type().clone(), false),
                &FunctionValueType::new(DataType::Float32, true),
                policy,
                allow,
                &Control,
            )
            .unwrap();
            value(&recipe, &a, 0, 0, policy, allow);
        }
    }
    if wide {
        for p in [1, 76] {
            let a = input(true, p, i8::MIN, vec![Some(1)]);
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for allow in [false, true] {
                    value(&r(&a, false, policy, allow), &a, 0, 0, policy, allow);
                }
            }
        }
        let a: ArrayRef = Arc::new(
            Decimal256Array::new(
                vec![i256::MIN, i256::MAX, i256::MIN].into(),
                Some(NullBuffer::from(vec![true, true, false])),
            )
            .with_precision_and_scale(76, 0)
            .unwrap(),
        );
        for row in 0..3 {
            value(
                &r(&a, true, DecimalOverflowPolicy::ReportError, true),
                &a,
                row,
                row,
                DecimalOverflowPolicy::ReportError,
                true,
            );
        }
    }
}
#[test]
fn decimal_float32_oracle_complete_decimal128_float32_profile() {
    complete(false);
}
#[test]
fn decimal_float32_oracle_complete_decimal256_float32_profile() {
    complete(true);
}
