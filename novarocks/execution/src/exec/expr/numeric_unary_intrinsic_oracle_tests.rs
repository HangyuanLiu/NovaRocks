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
//! Post-install child of legacy_numeric_unary_intrinsic_baseline_tests.
//! Expected values/errors always come from the original real arena shell.
use super::*;
use novarocks_functions::{
    EvaluatedArgument, KernelEvaluationControl, KernelFailure, PreparedNativeNegateRecipe,
    Selection,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, PureCompileControl, ValueLogicalType,
};
use std::time::Duration;
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
        panic!("native NEGATE does not wait")
    }
}
fn compare(input: ArrayRef) {
    let dtype = input.data_type().clone();
    let ty = if dtype == DataType::FixedSizeBinary(16) {
        FunctionValueType::try_with_logical_type(dtype.clone(), true, ValueLogicalType::LargeInt)
            .unwrap()
    } else {
        FunctionValueType::new(dtype.clone(), true)
    };
    let recipe = PreparedNativeNegateRecipe::try_new(&ty, &ty, &Control).unwrap();
    let all = Selection::all(input.len());
    let result = recipe
        .evaluate_selected(EvaluatedArgument::Column(&input), all, &Control)
        .unwrap();
    // E-D13 compares errors to original one-row invocations. A first full
    // batch error must not hide later successful selected rows.
    for row in 0..input.len() {
        match actual(input.slice(row, 1), true) {
            Ok(old) => {
                assert!(
                    !result
                        .errors()
                        .iter()
                        .any(|error| error.selected_ordinal() == row)
                );
                assert_eq!(result.values().slice(row, 1).to_data(), old.to_data());
            }
            Err(message) => assert_eq!(
                result
                    .errors()
                    .iter()
                    .find(|error| error.selected_ordinal() == row)
                    .unwrap()
                    .message(),
                message
            ),
        }
    }
    let selected = if input.len() > 1 {
        vec![1, input.len() - 1]
    } else {
        vec![]
    };
    // Keep sparse ordinals unique even for two-row sources.
    let mut selected = selected;
    selected.dedup();
    let selection = Selection::try_sparse(input.len(), &selected).unwrap();
    let sparse = recipe
        .evaluate_selected(EvaluatedArgument::Column(&input), selection, &Control)
        .unwrap();
    for (ordinal, &row) in selected.iter().enumerate() {
        match actual(input.slice(row, 1), false) {
            Ok(old) => assert_eq!(sparse.values().slice(ordinal, 1).to_data(), old.to_data()),
            Err(message) => assert_eq!(
                sparse
                    .errors()
                    .iter()
                    .find(|error| error.selected_ordinal() == ordinal)
                    .unwrap()
                    .message(),
                message
            ),
        }
    }
}
#[test]
fn native_negate_oracle_every_original_signed_float_largeint_with_sparse_null_and_ieee_edges() {
    for input in [
        Arc::new(Int8Array::from(vec![
            Some(i8::MIN),
            Some(-1),
            None,
            Some(0),
        ])) as ArrayRef,
        Arc::new(Int16Array::from(vec![
            Some(i16::MIN),
            Some(-1),
            None,
            Some(0),
        ])),
        Arc::new(Int32Array::from(vec![
            Some(i32::MIN),
            Some(-1),
            None,
            Some(0),
        ])),
        Arc::new(Int64Array::from(vec![
            Some(i64::MIN),
            Some(-1),
            None,
            Some(0),
        ])),
        Arc::new(Float32Array::from(vec![
            Some(0.0),
            Some(-0.0),
            None,
            Some(f32::NAN),
            Some(f32::INFINITY),
            Some(f32::NEG_INFINITY),
            Some(-101.9),
        ])),
        Arc::new(Float64Array::from(vec![
            Some(0.0),
            Some(-0.0),
            None,
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(-101.9),
        ])),
        largeint::array_from_i128(&[Some(i128::MIN), Some(-1), None, Some(0)]).unwrap(),
    ] {
        compare(input);
    }
}
#[test]
fn native_negate_oracle_full_original_decimal_precision_and_scale_carrier_domain() {
    for p in 1..=38 {
        for s in [i8::MIN, -1, 0, p as i8] {
            let input: ArrayRef = Arc::new(
                Decimal128Array::from(vec![Some(-1), None, Some(0)])
                    .with_precision_and_scale(p, s)
                    .unwrap(),
            );
            compare(input);
        }
    }
    for p in 1..=76 {
        for s in [i8::MIN, -1, 0, p as i8] {
            let input: ArrayRef = Arc::new(
                Decimal256Array::from(vec![Some(i256::MINUS_ONE), None, Some(i256::ZERO)])
                    .with_precision_and_scale(p, s)
                    .unwrap(),
            );
            compare(input);
        }
    }
}
