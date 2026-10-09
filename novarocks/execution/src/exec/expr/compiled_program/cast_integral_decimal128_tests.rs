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

//! Permanent actual typed Physical package -> LocalCompiler -> Frame differential.
//! Every declared signed source overload is independently named for RED receipts.
use super::*;
use crate::exec::expr::legacy_integral_decimal128_baseline_tests::{
    actual, integral_input, modes, values,
};
fn compare_integral(dtype: DataType) {
    for (policy, allow) in modes() {
        let target = DataType::Decimal128(4, 0);
        let full = integral_input(&dtype, vec![Some(-100), None, Some(100), Some(0)]);
        let program = compiled(
            FunctionValueType::new(dtype.clone(), true),
            FunctionValueType::new(target.clone(), true),
            Source::Column,
            Wrap::Bare,
            policy,
            allow,
        );
        for source in [full.clone(), full.slice(1, 3), full.slice(0, 0)] {
            let n = source.len();
            let b = RecordBatch::try_new(
                program.graph().nodes()[1].output_layout().schema().clone(),
                vec![
                    source.clone(),
                    Arc::new(Int64Array::from(vec![42; n])),
                    Arc::new(BooleanArray::from(vec![true; n])),
                ],
            )
            .unwrap();
            for rows in [
                (0..n).collect::<Vec<_>>(),
                (0..n).filter(|r| r % 2 == 0).collect(),
                Vec::new(),
            ] {
                let selection = Selection::try_sparse(n, &rows).unwrap();
                let out = instance(&program)
                    .evaluate(&b, selection, &Control)
                    .unwrap();
                assert_eq!(out.selection(), selection);
                assert_eq!(out.values().data_type(), &target);
                let mut wanted = Vec::new();
                let mut errors = Vec::new();
                for (ordinal, row) in rows.iter().copied().enumerate() {
                    match actual(source.slice(row, 1), target.clone(), policy, allow) {
                        Ok(array) => wanted.push(values(&array)[0]),
                        Err(message) => {
                            wanted.push(None);
                            errors.push((ordinal, message));
                        }
                    }
                }
                assert_eq!(values(out.values()), wanted);
                assert_eq!(
                    out.errors()
                        .iter()
                        .map(|e| (e.selected_ordinal(), e.message().to_owned()))
                        .collect::<Vec<_>>(),
                    errors
                );
            }
        }
    }
}
#[test]
fn integral_decimal128_actual_compiler_i8_to_4_0_policy_selection_and_carrier() {
    compare_integral(DataType::Int8);
}
#[test]
fn integral_decimal128_actual_compiler_i16_to_4_0_policy_selection_and_carrier() {
    compare_integral(DataType::Int16);
}
#[test]
fn integral_decimal128_actual_compiler_i32_to_4_0_policy_selection_and_carrier() {
    compare_integral(DataType::Int32);
}
#[test]
fn integral_decimal128_actual_compiler_i64_to_4_0_policy_selection_and_carrier() {
    compare_integral(DataType::Int64);
}
