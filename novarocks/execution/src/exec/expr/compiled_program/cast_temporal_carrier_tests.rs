// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::cast_timestamp_tests::timestamp_array;
use super::*;
use arrow::array::Date32Array;
use arrow::datatypes::TimeUnit;

#[test]
fn temporal_cast_actual_compiler_sparse_date_profiles_and_constant_producers() {
    for nullable in [false, true] {
        let dates: ArrayRef = Arc::new(Date32Array::from(if nullable {
            vec![Some(0), Some(-1), None, Some(1), Some(19782)]
        } else {
            vec![Some(0), Some(-1), Some(0), Some(1), Some(19782)]
        }));
        let rows = [1, 2, 3, 4];
        let selection = Selection::try_sparse(5, &rows).unwrap();
        for target in [
            DataType::Utf8,
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        ] {
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for allow in [false, true] {
                    let program = compiled(
                        FunctionValueType::new(DataType::Date32, nullable),
                        FunctionValueType::new(target.clone(), nullable),
                        Source::Column,
                        Wrap::Bare,
                        policy,
                        allow,
                    );
                    let schema = program.graph().nodes()[1].output_layout().schema().clone();
                    let batch = RecordBatch::try_new(
                        schema,
                        vec![
                            dates.clone(),
                            Arc::new(Int64Array::from(vec![42; 5])),
                            Arc::new(BooleanArray::from(vec![true; 5])),
                        ],
                    )
                    .unwrap();
                    let mut evaluator = instance(&program);
                    let result = evaluator.evaluate(&batch, selection, &Control).unwrap();
                    let legacy =
                        crate::exec::expr::cast::cast_with_special_rules(&dates, &target).unwrap();
                    let expected = arrow::compute::take(
                        legacy.as_ref(),
                        &arrow::array::UInt32Array::from(rows.map(|r| r as u32).to_vec()),
                        None,
                    )
                    .unwrap();
                    assert_eq!(result.values().to_data(), expected.to_data());
                    assert!(result.errors().is_empty());
                    let constant = compiled(
                        FunctionValueType::new(DataType::Date32, false),
                        FunctionValueType::new(target.clone(), false),
                        Source::Constant,
                        Wrap::Bare,
                        policy,
                        allow,
                    );
                    let schema = constant.graph().nodes()[1].output_layout().schema().clone();
                    let batch = RecordBatch::try_new(
                        schema,
                        vec![
                            Arc::new(Date32Array::from(vec![0; 5])),
                            Arc::new(Int64Array::from(vec![42; 5])),
                            Arc::new(BooleanArray::from(vec![true; 5])),
                        ],
                    )
                    .unwrap();
                    let result = instance(&constant)
                        .evaluate(&batch, selection, &Control)
                        .unwrap();
                    let array: ArrayRef = Arc::new(Date32Array::from(vec![71; 4]));
                    let expected =
                        crate::exec::expr::cast::cast_with_special_rules(&array, &target).unwrap();
                    assert_eq!(result.values().to_data(), expected.to_data());
                }
            }
        }
    }
}
#[test]
fn temporal_cast_actual_timestamp_date_profiles_skip_unselected_error_and_keep_selected_error() {
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for allow in [false, true] {
                let program = compiled(
                    FunctionValueType::new(DataType::Timestamp(unit, None), true),
                    FunctionValueType::new(DataType::Date32, true),
                    Source::Column,
                    Wrap::Bare,
                    policy,
                    allow,
                );
                let schema = program.graph().nodes()[1].output_layout().schema().clone();
                let source =
                    timestamp_array(unit, vec![Some(i64::MAX), Some(-1), None, Some(0), Some(1)]);
                let batch = RecordBatch::try_new(
                    schema,
                    vec![
                        source,
                        Arc::new(Int64Array::from(vec![42; 5])),
                        Arc::new(BooleanArray::from(vec![true; 5])),
                    ],
                )
                .unwrap();
                let rows = [1, 2, 3, 4];
                let result = instance(&program)
                    .evaluate(&batch, Selection::try_sparse(5, &rows).unwrap(), &Control)
                    .unwrap();
                assert_eq!(
                    result.values().to_data(),
                    Date32Array::from(vec![Some(-1), None, Some(0), Some(0)]).to_data()
                );
                assert!(result.errors().is_empty());
                if unit == TimeUnit::Second {
                    let result = instance(&program)
                        .evaluate(&batch, Selection::all(5), &Control)
                        .unwrap();
                    assert_eq!(result.errors().len(), 1);
                    assert_eq!(result.errors()[0].selected_ordinal(), 0);
                    assert_eq!(
                        result.errors()[0].message(),
                        "Cast error: Cannot convert arrow_array::types::TimestampSecondType 9223372036854775807 to datetime"
                    );
                }
            }
        }
    }
}
