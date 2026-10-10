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

//! Permanent exact Binary-to-Utf8 recipe equality against actual original arena CAST.
use super::legacy_binary_text_cast_baseline_tests::{actual, input};
use arrow::array::{Array, ArrayRef, BinaryArray, StringArray};
use arrow::datatypes::DataType;
use novarocks_functions::{
    CastOperation, CastRowResult, EvaluatedArgument, KernelEvaluationControl, KernelFailure,
    PreparedCastRecipe, SelectedValues, Selection,
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
        panic!("binary CAST never waits")
    }
}
#[test]
fn binary_text_oracle_exact_profile_source_nullability_policies_every_byte_slice_and_selection() {
    let values = input();
    for nullable in [false, true] {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for allow in [false, true] {
                let recipe = PreparedCastRecipe::try_new(
                    CastOperation::Carrier,
                    &FunctionValueType::new(DataType::Binary, nullable),
                    &FunctionValueType::new(DataType::Utf8, true),
                    policy,
                    allow,
                    &Control,
                )
                .unwrap();
                for source in [
                    values.clone(),
                    values.slice(0, 0),
                    Arc::new(BinaryArray::from(vec![Some(
                        (0u8..=255).collect::<Vec<_>>().as_slice(),
                    )])) as ArrayRef,
                ] {
                    let old = actual(&source, policy, allow).unwrap();
                    let old = old.as_any().downcast_ref::<StringArray>().unwrap();
                    for row in 0..source.len() {
                        if source.is_null(row) && !nullable {
                            continue;
                        }
                        let expected = if old.is_null(row) {
                            CastRowResult::Null
                        } else {
                            CastRowResult::Text(old.value(row).into())
                        };
                        assert_eq!(
                            recipe
                                .evaluate_row(
                                    EvaluatedArgument::Column(&source),
                                    row,
                                    row,
                                    &Control
                                )
                                .unwrap(),
                            expected
                        );
                    }
                }
                let rows = [0, 2];
                let selected = Selection::try_sparse(values.len(), &rows).unwrap();
                let compact_array: ArrayRef = Arc::new(BinaryArray::from(vec![
                    Some("你好\0🙂".as_bytes()),
                    Some(b"\xffa".as_slice()),
                ]));
                let compact = SelectedValues::try_new(
                    selected,
                    &DataType::Binary,
                    compact_array,
                    Box::default(),
                )
                .unwrap();
                for (ordinal, row) in selected.iter().enumerate() {
                    let old = actual(&values.slice(row, 1), policy, allow).unwrap();
                    let old = old.as_any().downcast_ref::<StringArray>().unwrap();
                    let expected = if old.is_null(0) {
                        CastRowResult::Null
                    } else {
                        CastRowResult::Text(old.value(0).into())
                    };
                    assert_eq!(
                        recipe
                            .evaluate_row(
                                EvaluatedArgument::Column(&values),
                                ordinal,
                                row,
                                &Control
                            )
                            .unwrap(),
                        expected
                    );
                    assert_eq!(
                        recipe
                            .evaluate_row(
                                EvaluatedArgument::SelectedColumn(&compact),
                                ordinal,
                                row,
                                &Control
                            )
                            .unwrap(),
                        expected
                    );
                }
            }
        }
    }
}
