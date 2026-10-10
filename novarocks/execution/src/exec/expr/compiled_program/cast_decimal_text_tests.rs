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

//! Actual LocalCompiler/LocalProgram decimal text selection and empty output.
use super::*;
use arrow::array::{Decimal128Array, Decimal256Array, StringArray};
use arrow_buffer::i256;

#[test]
fn decimal_text_actual_compiler_both_widths_scales_sparse_slice_and_empty() {
    for wide in [false, true] {
        for scale in [-3, 0, 2, if wide { 76 } else { 38 }] {
            let input: ArrayRef = if wide {
                Arc::new(
                    Decimal256Array::from(vec![
                        Some(i256::ZERO),
                        Some(i256::from_i128(5)),
                        None,
                        Some(i256::from_i128(-1)),
                    ])
                    .with_precision_and_scale(76, scale)
                    .unwrap(),
                )
            } else {
                Arc::new(
                    Decimal128Array::from(vec![Some(i128::MIN), Some(5), None, Some(-1)])
                        .with_precision_and_scale(38, scale)
                        .unwrap(),
                )
            };
            let input = input.slice(1, 3);
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for allow in [false, true] {
                    let program = compiled(
                        FunctionValueType::new(input.data_type().clone(), true),
                        FunctionValueType::new(DataType::Utf8, true),
                        Source::Column,
                        Wrap::Bare,
                        policy,
                        allow,
                    );
                    let schema = program.graph().nodes()[1].output_layout().schema().clone();
                    let batch = RecordBatch::try_new(
                        schema,
                        vec![
                            input.clone(),
                            Arc::new(Int64Array::from(vec![42; 3])),
                            Arc::new(BooleanArray::from(vec![true; 3])),
                        ],
                    )
                    .unwrap();
                    let rows = [0, 1, 2];
                    let selection = Selection::try_sparse(3, &rows).unwrap();
                    let mut evaluator = instance(&program);
                    let snapshot = program
                        .checked()
                        .channels()
                        .expressions()
                        .resolved_calls()
                        .snapshot();
                    let occurrence = ProgramUseRef {
                        arena: root().arena(),
                        use_id: snapshot.bindings()[&root()],
                    };
                    let summary = evaluator.effects[&occurrence];
                    assert_eq!(
                        summary
                            .for_use(summary.context())
                            .unwrap()
                            .may_raise_row_error,
                        !wide && scale > 0,
                    );
                    let result = evaluator.evaluate(&batch, selection, &Control).unwrap();
                    let old =
                        crate::exec::expr::cast::cast_with_special_rules(&input, &DataType::Utf8)
                            .unwrap();
                    assert_eq!(result.values().to_data(), old.to_data());
                    assert!(result.errors().is_empty());
                    let result = evaluator
                        .evaluate(&batch, Selection::try_sparse(3, &[]).unwrap(), &Control)
                        .unwrap();
                    assert_eq!(
                        result.values().to_data(),
                        StringArray::from(Vec::<Option<&str>>::new()).to_data()
                    );
                    assert!(result.errors().is_empty());
                }
            }
        }
    }
}
