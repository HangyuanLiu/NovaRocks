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

use super::*;
use crate::PreparedArithmeticRecipe;
use arrow_array::{Array, ArrayRef, Float32Array, Float64Array};
use novarocks_type_contract::{CompileControlError, CompilePhase};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
#[derive(Default)]
struct Compile {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Compile {
    fn checkpoint(&self, _: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        let mut t = self.trace.lock().unwrap();
        let at = t.len();
        t.push(n);
        if let Some((target, cause)) = self.refusal
            && at == target
        {
            Err(cause)
        } else {
            Ok(())
        }
    }
}
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        let mut t = self.trace.lock().unwrap();
        let at = t.len();
        t.push(n);
        if let Some((target, cause)) = &self.refusal
            && at == *target
        {
            Err(cause.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("Float arithmetic never waits")
    }
}
const OPS: [ArithmeticOperator; 5] = [
    ArithmeticOperator::Add,
    ArithmeticOperator::Subtract,
    ArithmeticOperator::Multiply,
    ArithmeticOperator::Divide,
    ArithmeticOperator::Modulo,
];
fn prepare(op: ArithmeticOperator, left: DataType, right: DataType) -> PreparedArithmeticRecipe {
    PreparedArithmeticRecipe::try_new(
        op,
        &FunctionValueType::new(left, true),
        &FunctionValueType::new(right, true),
        &FunctionValueType::new(DataType::Float64, true),
        DecimalOverflowPolicy::OutputNull,
        false,
        &Compile::default(),
    )
    .unwrap()
}
#[test]
fn float_arithmetic_recipe_consumes_original_arrow_math_and_width_zero_policy() {
    let left: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(1.0),
        Some(-0.0),
        Some(f64::INFINITY),
        Some(f64::from_bits(0x7ff8123456789abc)),
        None,
        Some(9.0),
    ]));
    let peers: Vec<ArrayRef> = vec![
        Arc::new(Float32Array::from(vec![
            Some(0.0),
            Some(-0.0),
            Some(1.0),
            Some(3.0),
            Some(7.0),
            None,
        ])),
        Arc::new(Float64Array::from(vec![
            Some(0.0),
            Some(-0.0),
            Some(1.0),
            Some(3.0),
            Some(7.0),
            None,
        ])),
    ];
    for right in peers {
        for op in OPS {
            let recipe = prepare(op, left.data_type().clone(), right.data_type().clone());
            let expected = match op {
                ArithmeticOperator::Add => crate::legacy_arithmetic::eval_add_arrays(
                    left.clone(),
                    right.clone(),
                    DataType::Float64,
                    false,
                    DecimalOverflowPolicy::OutputNull,
                ),
                ArithmeticOperator::Subtract => crate::legacy_arithmetic::eval_sub_arrays(
                    left.clone(),
                    right.clone(),
                    DataType::Float64,
                    false,
                    DecimalOverflowPolicy::OutputNull,
                ),
                ArithmeticOperator::Multiply => crate::legacy_arithmetic::eval_mul_arrays(
                    left.clone(),
                    right.clone(),
                    DataType::Float64,
                    false,
                    DecimalOverflowPolicy::OutputNull,
                ),
                ArithmeticOperator::Divide => crate::legacy_arithmetic::eval_div_arrays(
                    left.clone(),
                    right.clone(),
                    &DataType::Float64,
                    false,
                    DecimalOverflowPolicy::OutputNull,
                ),
                ArithmeticOperator::Modulo => crate::legacy_arithmetic::eval_mod_arrays(
                    left.clone(),
                    right.clone(),
                    DataType::Float64,
                    false,
                    DecimalOverflowPolicy::OutputNull,
                ),
            }
            .unwrap();
            let expected = expected.as_any().downcast_ref::<Float64Array>().unwrap();
            for row in 0..left.len() {
                let actual = recipe
                    .evaluate_row(
                        EvaluatedArgument::Column(&left),
                        row,
                        row,
                        EvaluatedArgument::Column(&right),
                        row,
                        row,
                        &Control::default(),
                    )
                    .unwrap();
                if expected.is_null(row) {
                    assert_eq!(actual, ArithmeticRowResult::Null);
                } else {
                    let ArithmeticRowResult::Float(value) = actual else {
                        panic!("original F64 expected")
                    };
                    assert_eq!(value.to_bits(), expected.value(row).to_bits());
                }
            }
        }
    }
}
#[test]
fn float_arithmetic_recipe_full_fvt_result_and_nominal_boundary_is_exact() {
    for ty in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Decimal128(1, -128),
        DataType::Decimal128(38, 38),
    ] {
        for op in OPS {
            for (left, right) in [
                (DataType::Float32, ty.clone()),
                (ty.clone(), DataType::Float64),
            ] {
                let recipe = prepare(op, left.clone(), right.clone());
                assert_eq!(recipe.left_type(), &FunctionValueType::new(left, true));
                assert_eq!(recipe.right_type(), &FunctionValueType::new(right, true));
                assert_eq!(
                    recipe.result_type(),
                    &FunctionValueType::new(DataType::Float64, true)
                );
            }
        }
    }
    let f = FunctionValueType::new(DataType::Float64, true);
    for other in [
        DataType::UInt64,
        DataType::Float16,
        DataType::Decimal256(76, 1),
        DataType::Boolean,
        DataType::FixedSizeBinary(16),
    ] {
        assert_eq!(
            PreparedArithmeticRecipe::try_new(
                ArithmeticOperator::Add,
                &f,
                &FunctionValueType::new(other, true),
                &f,
                DecimalOverflowPolicy::OutputNull,
                false,
                &Compile::default()
            ),
            Err(ArithmeticPrepareError::Unsupported)
        );
    }
    assert_eq!(
        PreparedArithmeticRecipe::try_new(
            ArithmeticOperator::Add,
            &f,
            &f,
            &FunctionValueType::new(DataType::Int64, true),
            DecimalOverflowPolicy::OutputNull,
            false,
            &Compile::default()
        ),
        Err(ArithmeticPrepareError::TypeMismatch)
    );
    assert_eq!(
        PreparedArithmeticRecipe::try_new(
            ArithmeticOperator::Add,
            &f,
            &f,
            &FunctionValueType::new(DataType::Float64, false),
            DecimalOverflowPolicy::OutputNull,
            false,
            &Compile::default()
        ),
        Err(ArithmeticPrepareError::TypeMismatch)
    );
}
#[test]
fn float_arithmetic_recipe_three_compile_and_seven_runtime_causes_have_no_tail() {
    let f = FunctionValueType::new(DataType::Float64, true);
    let make = |c: &dyn PureCompileControl| {
        PreparedArithmeticRecipe::try_new(
            ArithmeticOperator::Add,
            &f,
            &f,
            &f,
            DecimalOverflowPolicy::OutputNull,
            false,
            c,
        )
    };
    let normal = Compile::default();
    let recipe = make(&normal).unwrap();
    let trace = normal.trace.lock().unwrap().clone();
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 0..trace.len() {
            let c = Compile {
                refusal: Some((at, cause)),
                ..Compile::default()
            };
            assert_eq!(make(&c).unwrap_err().control_error(), Some(cause));
            assert_eq!(*c.trace.lock().unwrap(), trace[..=at]);
        }
    }
    let left: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.0)]));
    let right: ArrayRef = Arc::new(Float64Array::from(vec![Some(2.0)]));
    let run = |c: &dyn KernelEvaluationControl| {
        recipe.evaluate_row(
            EvaluatedArgument::Column(&left),
            0,
            0,
            EvaluatedArgument::Column(&right),
            0,
            0,
            c,
        )
    };
    let normal = Control::default();
    assert_eq!(run(&normal).unwrap(), ArithmeticRowResult::Float(3.0));
    let trace = normal.trace.lock().unwrap().clone();
    for cause in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(crate::KernelDiagnostic::new("actual Float control")),
        KernelFailure::InstanceFailed,
        KernelFailure::Internal(crate::KernelDiagnostic::new("actual Float control")),
        KernelFailure::Operational(crate::KernelDiagnostic::new("actual Float control")),
    ] {
        for at in 0..trace.len() {
            let c = Control {
                refusal: Some((at, cause.clone())),
                ..Control::default()
            };
            assert_eq!(run(&c), Err(cause.clone()));
            assert_eq!(*c.trace.lock().unwrap(), trace[..=at]);
        }
    }
}
