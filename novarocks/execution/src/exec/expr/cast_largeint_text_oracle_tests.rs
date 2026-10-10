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
//! Independent original v1 versus exact Physical LARGEINT text recipe.
use super::cast::cast_with_special_rules;
use arrow::array::{Array, ArrayRef, StringArray};
use arrow::datatypes::DataType;
use novarocks_functions::{
    CastOperation, CastRowResult, EvaluatedArgument, KernelEvaluationControl, KernelFailure,
    PreparedCastRecipe,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, DecimalOverflowPolicy, FunctionValueType, PureCompileControl,
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
        panic!("LARGEINT text never waits")
    }
}
#[test]
fn largeint_text_original_v1_all_policy_nullability_min_max_slice_and_empty() {
    for nullable in [false, true] {
        let mut values = vec![
            Some(i128::MIN),
            Some(i128::MAX),
            Some(-1),
            Some(0),
            Some(42),
        ];
        if nullable {
            values.push(None);
        }
        let input = novarocks_types::largeint::array_from_i128(&values).unwrap();
        for source in [input.clone(), input.slice(1, 3), input.slice(0, 0)] {
            let old = cast_with_special_rules(&source, &DataType::Utf8).unwrap();
            let old = old.as_any().downcast_ref::<StringArray>().unwrap();
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for allow in [false, true] {
                    let r = PreparedCastRecipe::try_new(
                        CastOperation::Carrier,
                        &FunctionValueType::new(DataType::FixedSizeBinary(16), nullable),
                        &FunctionValueType::new(DataType::Utf8, nullable),
                        policy,
                        allow,
                        &Control,
                    )
                    .unwrap();
                    for row in 0..source.len() {
                        let expected = if old.is_null(row) {
                            CastRowResult::Null
                        } else {
                            CastRowResult::Text(old.value(row).into())
                        };
                        assert_eq!(
                            r.evaluate_row(EvaluatedArgument::Column(&source), row, row, &Control)
                                .unwrap(),
                            expected
                        );
                    }
                }
            }
        }
    }
}
#[test]
fn largeint_text_shared_raw_adapter_retains_original_full_errors() {
    let wrong: ArrayRef = std::sync::Arc::new(arrow::array::Int64Array::from(vec![1]));
    assert_eq!(
        novarocks_functions::largeint_text::cast_array(&wrong).unwrap_err(),
        "cast LARGEINT to VARCHAR: expected FixedSizeBinaryArray"
    );
    let arr = novarocks_types::largeint::array_from_i128(&[Some(-1)]).unwrap();
    assert_eq!(
        novarocks_functions::largeint_text::cast_array(&arr)
            .unwrap()
            .to_data(),
        cast_with_special_rules(&arr, &DataType::Utf8)
            .unwrap()
            .to_data()
    );
}
