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
//! Permanent recipe differential against the original real arena TIME shell.
//! Before installation each of three independent tests fails at exact preparation.
use super::legacy_text_time_cast_baseline_tests::{actual, corpus, input, modes, values};
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{DataType, TimeUnit};
use novarocks_functions::{
    CastOperation, CastRowResult, EvaluatedArgument, KernelEvaluationControl, KernelFailure,
    PreparedCastRecipe, Selection,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
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
        panic!("TIME cast does not wait")
    }
}
fn row_value(
    recipe: &PreparedCastRecipe,
    argument: EvaluatedArgument<'_>,
    ordinal: usize,
    row: usize,
) -> Option<i64> {
    match recipe
        .evaluate_row(argument, ordinal, row, &Control)
        .unwrap()
    {
        CastRowResult::Null => None,
        CastRowResult::Signed(value) => Some(value),
        other => panic!("TIME recipe returned a different frozen row category: {other:?}"),
    }
}
fn compare(dtype: DataType) {
    for (allow, policy) in modes() {
        for nullable in [true, false] {
            let source = FunctionValueType::new(dtype.clone(), nullable);
            let result = FunctionValueType::new(DataType::Time64(TimeUnit::Microsecond), true);
            // Same actual SQL effect-author operation; no default operation replacement.
            let recipe = PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &source,
                &result,
                policy,
                allow,
                &Control,
            )
            .unwrap();
            for array in [
                corpus(&dtype),
                corpus(&dtype).slice(1, 7),
                input(&dtype, &[]),
            ] {
                let expected = values(&actual(array.clone(), allow, policy, false).unwrap());
                for (ordinal, row) in Selection::all(array.len()).iter().enumerate() {
                    if !nullable && array.is_null(row) {
                        continue;
                    }
                    assert_eq!(
                        row_value(&recipe, EvaluatedArgument::Column(&array), ordinal, row),
                        expected[row]
                    );
                }
                let rows: Vec<_> = (0..array.len())
                    .filter(|row| row % 3 == 0 && (nullable || !array.is_null(*row)))
                    .collect();
                let selection = Selection::try_sparse(array.len(), &rows).unwrap();
                for (ordinal, row) in selection.iter().enumerate() {
                    assert_eq!(
                        row_value(&recipe, EvaluatedArgument::Column(&array), ordinal, row),
                        expected[row]
                    );
                }
            }
            let scalar: ArrayRef = input(&dtype, &[Some("01:02:03")]);
            let expected = values(&actual(scalar.clone(), allow, policy, false).unwrap())[0];
            for row in 0..17 {
                assert_eq!(
                    row_value(&recipe, EvaluatedArgument::Scalar(&scalar), row, row),
                    expected
                );
            }
            assert_eq!(recipe.source_type(), &source);
            assert_eq!(recipe.result_type(), &result);
        }
    }
}
#[test]
fn text_time_recipe_differential_utf8_exact_profile() {
    compare(DataType::Utf8);
}
#[test]
fn text_time_recipe_differential_large_utf8_exact_profile() {
    compare(DataType::LargeUtf8);
}
#[test]
fn text_time_recipe_differential_utf8_view_exact_profile() {
    compare(DataType::Utf8View);
}
