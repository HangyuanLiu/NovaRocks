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

//! Actual LocalCompiler -> checked LocalProgram -> Frame Decimal Float64 profile witnesses.
use super::*;
use crate::exec::expr::legacy_decimal_float_cast_baseline_tests::{actual, input};
use arrow::array::{Float64Array, UInt32Array};
fn batch(program: &LocalProgram, a: ArrayRef) -> RecordBatch {
    let len = a.len();
    RecordBatch::try_new(
        program.graph().nodes()[1].output_layout().schema().clone(),
        vec![
            a,
            Arc::new(Int64Array::from(vec![42; len])),
            Arc::new(BooleanArray::from(vec![true; len])),
        ],
    )
    .unwrap()
}
fn full(wide: bool) {
    for p in [1, if wide { 76 } else { 38 }] {
        // PhysicalPlan resource.rs admits -max_scale..max_scale;
        // the wider original Arrow/FVT domain stays covered by direct recipe oracles.
        let max_scale = if wide { 76 } else { 38 };
        for scale in [-max_scale, -3, 0, 1, p as i8] {
            for nullable in [false, true] {
                for policy in [
                    DecimalOverflowPolicy::OutputNull,
                    DecimalOverflowPolicy::ReportError,
                ] {
                    for allow in [false, true] {
                        let a = input(
                            wide,
                            p,
                            scale,
                            if nullable {
                                vec![Some(0), Some(1), None, Some(-1), Some(i128::MAX)]
                            } else {
                                vec![Some(0), Some(1), Some(0), Some(-1), Some(i128::MAX)]
                            },
                        )
                        .slice(1, 4);
                        let program = compiled(
                            FunctionValueType::new(a.data_type().clone(), nullable),
                            FunctionValueType::new(DataType::Float64, nullable),
                            Source::Column,
                            Wrap::Bare,
                            policy,
                            allow,
                        );
                        let rows = [0, 1, 3];
                        let b = batch(&program, a.clone());
                        let out = instance(&program)
                            .evaluate(&b, Selection::try_sparse(4, &rows).unwrap(), &Control)
                            .unwrap();
                        let old = actual(&a, policy, allow).unwrap();
                        let expected = arrow::compute::take(
                            old.as_ref(),
                            &UInt32Array::from(rows.map(|r| r as u32).to_vec()),
                            None,
                        )
                        .unwrap();
                        assert_eq!(out.values().to_data(), expected.to_data());
                        assert!(out.errors().is_empty());
                        let empty = instance(&program)
                            .evaluate(&b, Selection::try_sparse(4, &[]).unwrap(), &Control)
                            .unwrap();
                        assert_eq!(empty.values().data_type(), &DataType::Float64);
                        assert_eq!(empty.values().len(), 0);
                        assert!(empty.errors().is_empty());
                        let constant = compiled(
                            FunctionValueType::new(a.data_type().clone(), false),
                            FunctionValueType::new(DataType::Float64, false),
                            Source::Constant,
                            Wrap::Bare,
                            policy,
                            allow,
                        );
                        let cb = batch(&constant, input(wide, p, scale, vec![Some(0); 4]));
                        let expanded = input(wide, p, scale, vec![Some(1); 3]);
                        let out = instance(&constant)
                            .evaluate(&cb, Selection::try_sparse(4, &rows).unwrap(), &Control)
                            .unwrap();
                        assert_eq!(
                            out.values().to_data(),
                            actual(&expanded, policy, allow).unwrap().to_data()
                        );
                    }
                }
            }
        }
    }
}
#[test]
fn decimal_float_actual_compiler_decimal128_float64() {
    full(false);
}
#[test]
fn decimal_float_actual_compiler_decimal256_float64() {
    full(true);
}

#[test]
fn decimal_float_actual_physical_source_rejects_out_of_range_scale_before_compile() {
    for wide in [false, true] {
        let max = if wide { 76 } else { 38 };
        let source = if wide {
            DataType::Decimal256(1, -127)
        } else {
            DataType::Decimal128(1, -127)
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            fixture(
                FunctionValueType::new(source, true),
                FunctionValueType::new(DataType::Float64, true),
                Source::Column,
                Wrap::Bare,
                DecimalOverflowPolicy::ReportError,
                true,
            )
        }));
        let Err(failure) = result else {
            panic!("original Physical package must reject its illegal source scale");
        };
        let message = failure
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| failure.downcast_ref::<&str>().copied())
            .expect("original unwrap string payload");
        assert!(
            message.contains(&format!(
                "Arrow decimal precision/scale (1, -127) is outside 1..={max} and -{max}..={max}"
            )),
            "{message}"
        );
    }
}

#[test]
fn decimal_float_actual_constant_source_rejects_out_of_precision_payload() {
    for wide in [false, true] {
        let ty = FunctionValueType::new(
            if wide {
                DataType::Decimal256(1, 0)
            } else {
                DataType::Decimal128(1, 0)
            },
            false,
        );
        let field = Arc::new(ty.try_to_field("original-decimal-constant").unwrap());
        let result = if wide {
            novarocks_functions::ConstantValue::from_decimal256_be(
                field,
                ty,
                arrow_buffer::i256::from_i128(71).to_be_bytes(),
                options().constants,
                CompilePhase::FunctionSpecialization,
                &Control,
            )
        } else {
            novarocks_functions::ConstantValue::from_decimal128(
                field,
                ty,
                71,
                options().constants,
                CompilePhase::FunctionSpecialization,
                &Control,
            )
        };
        let message = result.unwrap_err().to_string();
        assert!(
            message.contains("precision 1") && message.contains("too large"),
            "{message}"
        );
    }
}
