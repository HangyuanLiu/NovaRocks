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
//! Actual closed LocalCompiler/Frame source contract; Physical scale admission is narrower than direct Arrow/FVT metadata.
use super::*;
use crate::exec::expr::legacy_float64_decimal128_baseline_tests::{actual, input, modes, values};
fn batch(program: &LocalProgram, a: ArrayRef) -> RecordBatch {
    let n = a.len();
    RecordBatch::try_new(
        program.graph().nodes()[1].output_layout().schema().clone(),
        vec![
            a,
            Arc::new(Int64Array::from(vec![42; n])),
            Arc::new(BooleanArray::from(vec![true; n])),
        ],
    )
    .unwrap()
}
#[test]
fn float64_decimal128_actual_compiler_complete_physical_profiles_and_constant_author() {
    for (p, s) in [(1, -38), (1, 0), (1, 1), (18, 4), (38, -38), (38, 38)] {
        for (policy, allow) in modes() {
            let target = DataType::Decimal128(p, s);
            let program = compiled(
                FunctionValueType::new(DataType::Float64, true),
                FunctionValueType::new(target.clone(), true),
                Source::Column,
                Wrap::Bare,
                policy,
                allow,
            );
            let a = input(vec![
                Some(99.0),
                Some(1.25),
                None,
                Some(-1.25),
                Some(f64::NAN),
                Some(f64::INFINITY),
            ])
            .slice(1, 5);
            let data = batch(&program, a.clone());
            let rows = [0, 1, 3, 4];
            let selection = Selection::try_sparse(5, &rows).unwrap();
            let out = instance(&program)
                .evaluate(&data, selection, &Control)
                .unwrap();
            assert_eq!(out.selection(), selection);
            let mut expected = Vec::new();
            let mut expected_errors = Vec::new();
            for (ordinal, row) in rows.iter().copied().enumerate() {
                // E-D13 compares each real selected site against the unchanged
                // actual arena oracle; unselected batch failures are not demand.
                match actual(a.slice(row, 1), p, s, policy, allow) {
                    Ok(value) => expected.push(values(&value)[0]),
                    Err(message) => {
                        expected.push(None);
                        expected_errors.push((ordinal, message));
                    }
                }
            }
            assert_eq!(values(out.values()), expected);
            assert_eq!(out.errors().len(), expected_errors.len());
            for (error, (ordinal, message)) in out.errors().iter().zip(expected_errors) {
                assert_eq!(error.selected_ordinal(), ordinal);
                assert_eq!(error.message(), message);
            }
            assert_eq!(out.values().data_type(), &target);
            let empty = instance(&program)
                .evaluate(&data, Selection::try_sparse(5, &[]).unwrap(), &Control)
                .unwrap();
            assert!(empty.values().is_empty());
            assert!(empty.errors().is_empty());
            let program = compiled(
                FunctionValueType::new(DataType::Float64, false),
                FunctionValueType::new(target, true),
                Source::Constant,
                Wrap::Bare,
                policy,
                allow,
            );
            let data = batch(&program, input(vec![Some(7.0); 5]));
            let out = instance(&program)
                .evaluate(&data, selection, &Control)
                .unwrap();
            // Parent source author emits the exact Float64 -0.0 literal, not
            // a value guessed from the sample batch or decimal target precision.
            let expected = actual(input(vec![Some(-0.0); 4]), p, s, policy, allow).unwrap();
            assert_eq!(values(out.values()), values(&expected));
            assert!(out.errors().is_empty());
        }
    }
}
