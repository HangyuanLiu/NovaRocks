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
//! Permanent accurate Float64-to-Decimal128 profile oracle, including original selected panic/NULL and full short batch messages.
use super::legacy_float64_decimal128_baseline_tests::{actual, input, modes, values};
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::DataType;
use novarocks_functions::{
    CastOperation, CastRowResult, ConstantPool, EvaluatedArgument, KernelEvaluationControl,
    KernelFailure, PreparedCastRecipe, SelectedValues, Selection,
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
        panic!("Float Decimal CAST never waits")
    }
}
fn recipe(
    p: u8,
    s: i8,
    nullable: bool,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> PreparedCastRecipe {
    PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &FunctionValueType::new(DataType::Float64, nullable),
        &FunctionValueType::new(DataType::Decimal128(p, s), true),
        policy,
        allow,
        &Control,
    )
    .unwrap()
}
fn compare(
    r: &PreparedCastRecipe,
    a: &ArrayRef,
    ordinal: usize,
    row: usize,
    p: u8,
    s: i8,
    policy: DecimalOverflowPolicy,
    allow: bool,
) {
    let old = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        actual(a.slice(row, 1), p, s, policy, allow)
    }));
    let pure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        r.evaluate_row(EvaluatedArgument::Column(a), ordinal, row, &Control)
            .unwrap()
    }));
    assert_eq!(
        old.is_err(),
        pure.is_err(),
        "original active-row panic remains unchanged"
    );
    if let (Ok(old), Ok(pure)) = (old, pure) {
        match old {
            Ok(out) => match values(&out)[0] {
                None => assert_eq!(pure, CastRowResult::Null),
                Some(v) => assert_eq!(pure, CastRowResult::Decimal128(v)),
            },
            Err(message) => {
                let CastRowResult::RowError(err) = pure else {
                    panic!("original batch error must retain exact selected row recipe")
                };
                assert_eq!(err.message(), message);
                assert_eq!(err.selected_ordinal(), ordinal);
            }
        }
    }
}
#[test]
fn float64_decimal128_permanent_full_precision_signed_scale_profile() {
    // Every legal Arrow/FVT p/s is admitted. Static policy-independent math is
    // checked over the complete axis; all four original policies are separately
    // checked on boundary/representative profiles below rather than repeated here.
    for p in 1..=38 {
        for s in i8::MIN..=p as i8 {
            for nullable in [false, true] {
                let r = recipe(p, s, nullable, DecimalOverflowPolicy::OutputNull, false);
                if s == i8::MIN && ![1, 38].contains(&p) {
                    continue;
                }
                let a = input(if nullable {
                    vec![Some(1.0), Some(-1.0), None, Some(f64::NAN)]
                } else {
                    vec![Some(1.0), Some(-1.0), Some(0.0), Some(f64::INFINITY)]
                });
                for row in 0..a.len() {
                    compare(
                        &r,
                        &a,
                        row,
                        row,
                        p,
                        s,
                        DecimalOverflowPolicy::OutputNull,
                        false,
                    );
                }
            }
        }
    }
}
#[test]
fn float64_decimal128_permanent_policy_selected_slice_scalar_constant_and_original_min() {
    for (policy, allow) in modes() {
        for (p, s) in [(1, 0), (18, 4), (38, -38), (38, 38), (38, -39), (1, -128)] {
            let a = input(vec![
                Some(99.0),
                Some(1.25),
                None,
                Some(-1.25),
                Some(f64::INFINITY),
            ])
            .slice(1, 4);
            let r = recipe(p, s, true, policy, allow);
            for (ordinal, row) in Selection::try_sparse(4, &[0, 1, 3])
                .unwrap()
                .iter()
                .enumerate()
            {
                compare(&r, &a, ordinal, row, p, s, policy, allow);
            }
            if s != i8::MIN {
                let selection = Selection::try_sparse(4, &[0, 1, 3]).unwrap();
                let compact = SelectedValues::try_new(
                    selection,
                    &DataType::Float64,
                    input(vec![Some(1.25), None, Some(f64::INFINITY)]),
                    Box::default(),
                )
                .unwrap();
                for (ordinal, row) in selection.iter().enumerate() {
                    assert_eq!(
                        r.evaluate_row(
                            EvaluatedArgument::SelectedColumn(&compact),
                            ordinal,
                            row,
                            &Control
                        )
                        .unwrap(),
                        r.evaluate_row(EvaluatedArgument::Column(&a), ordinal, row, &Control)
                            .unwrap()
                    );
                }
                let scalar = a.slice(0, 1);
                assert_eq!(
                    r.evaluate_row(EvaluatedArgument::Scalar(&scalar), 88, 99, &Control)
                        .unwrap(),
                    r.evaluate_row(EvaluatedArgument::Column(&a), 88, 0, &Control)
                        .unwrap()
                );
            }
        }
        let a = input(vec![Some(i128::MIN as f64)]);
        compare(
            &recipe(38, 0, false, policy, allow),
            &a,
            0,
            0,
            38,
            0,
            policy,
            allow,
        );
        let ty = FunctionValueType::new(DataType::Float64, false);
        let a = input(vec![Some(i128::MIN as f64), Some(1.25)]);
        let pool = ConstantPool::try_new(
            Arc::new(ty.try_to_field("exact-pool").unwrap()),
            ty.clone(),
            a.to_data(),
            super::pure_differential::constant_policy(),
            CompilePhase::FunctionSpecialization,
            &Control,
        )
        .unwrap();
        let r = recipe(18, 1, false, policy, allow);
        assert_eq!(
            r.evaluate_row(
                EvaluatedArgument::Constant(&pool.value(1).unwrap()),
                5,
                100,
                &Control
            )
            .unwrap(),
            CastRowResult::Decimal128(13)
        );
    }
}
