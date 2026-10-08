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

//! Independent actual v1/recipe decimal text differential and old panic goldens.
use super::cast::cast_with_special_rules;
use arrow::array::{Array, ArrayRef, Decimal128Array, Decimal256Array, StringArray};
use arrow::datatypes::DataType;
use arrow_buffer::i256;
use novarocks_functions::{
    CastOperation, CastRowResult, EvaluatedArgument, KernelEvaluationControl, KernelFailure,
    PreparedCastRecipe, Selection,
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
        panic!("decimal text never waits")
    }
}
fn raw(wide: bool, p: u8, s: i8, values: Vec<Option<i128>>) -> ArrayRef {
    if wide {
        Arc::new(
            Decimal256Array::from(
                values
                    .into_iter()
                    .map(|v| v.map(i256::from_i128))
                    .collect::<Vec<_>>(),
            )
            .with_precision_and_scale(p, s)
            .unwrap(),
        )
    } else {
        Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(p, s)
                .unwrap(),
        )
    }
}
fn recipe(input: &ArrayRef, policy: DecimalOverflowPolicy, allow: bool) -> PreparedCastRecipe {
    PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &FunctionValueType::new(input.data_type().clone(), true),
        &FunctionValueType::new(DataType::Utf8, true),
        policy,
        allow,
        &Control,
    )
    .unwrap()
}
fn compare(input: &ArrayRef, policy: DecimalOverflowPolicy, allow: bool) {
    compare_with_nullability(input, true, policy, allow);
}
fn compare_with_nullability(
    input: &ArrayRef,
    nullable: bool,
    policy: DecimalOverflowPolicy,
    allow: bool,
) {
    let actual = cast_with_special_rules(input, &DataType::Utf8).unwrap();
    let old = actual.as_any().downcast_ref::<StringArray>().unwrap();
    let r = PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &FunctionValueType::new(input.data_type().clone(), nullable),
        &FunctionValueType::new(DataType::Utf8, nullable),
        policy,
        allow,
        &Control,
    )
    .unwrap();
    for row in 0..input.len() {
        let expected = if old.is_null(row) {
            CastRowResult::Null
        } else {
            CastRowResult::Text(old.value(row).into())
        };
        assert_eq!(
            r.evaluate_row(EvaluatedArgument::Column(input), row, row, &Control)
                .unwrap(),
            expected
        );
    }
}
#[test]
fn decimal_text_oracle_all_legal_precision_scale_metadata_matrix() {
    for wide in [false, true] {
        let max = if wide { 76 } else { 38 };
        for p in 1..=max {
            for s in i8::MIN..=p as i8 {
                for nullable in [false, true] {
                    let mut values = vec![Some(0), Some(1), Some(-1)];
                    if nullable {
                        values.push(None);
                    }
                    let input = raw(wide, p, s, values);
                    for policy in [
                        DecimalOverflowPolicy::OutputNull,
                        DecimalOverflowPolicy::ReportError,
                    ] {
                        for allow in [false, true] {
                            compare_with_nullability(&input, nullable, policy, allow);
                        }
                    }
                }
            }
        }
    }
}
#[test]
fn decimal_text_oracle_slice_empty_and_unselected_min_preserve_v1_visitation() {
    for wide in [false, true] {
        let input = raw(
            wide,
            if wide { 76 } else { 38 },
            2,
            vec![Some(i128::MIN), Some(5), None],
        );
        for selected in [input.slice(1, 2), input.slice(0, 0)] {
            compare(&selected, DecimalOverflowPolicy::ReportError, true);
        }
        let r = recipe(&input, DecimalOverflowPolicy::ReportError, true);
        let rows = [1, 2];
        let selection = Selection::try_sparse(3, &rows).unwrap();
        for (ordinal, row) in selection.iter().enumerate() {
            let expected = if row == 1 {
                CastRowResult::Text("0.05".into())
            } else {
                CastRowResult::Null
            };
            assert_eq!(
                r.evaluate_row(EvaluatedArgument::Column(&input), ordinal, row, &Control)
                    .unwrap(),
                expected
            );
        }
    }
}
#[test]
fn decimal_text_oracle_original_decimal128_min_panic_or_wrapping_is_not_row_error_translation() {
    let input = raw(false, 38, 2, vec![Some(i128::MIN)]);
    let r = recipe(&input, DecimalOverflowPolicy::OutputNull, false);
    let old = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        cast_with_special_rules(&input, &DataType::Utf8).unwrap()
    }));
    let prepared = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        r.evaluate_row(EvaluatedArgument::Column(&input), 0, 0, &Control)
            .unwrap()
    }));
    if cfg!(debug_assertions) {
        assert!(old.is_err());
        assert!(prepared.is_err());
    } else {
        assert_eq!(
            old.unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "--1701411834604692317316873037158841057.28"
        );
        assert_eq!(
            prepared.unwrap(),
            CastRowResult::Text("--1701411834604692317316873037158841057.28".into())
        );
    }
}
#[test]
fn decimal_text_oracle_original_full_width_decimal256_min_double_sign() {
    let minimum = i256::from_string(
        "-57896044618658097711785492504343953926634992332820282019728792003956564819968",
    )
    .unwrap();
    for s in [-3, 0, 2, 76] {
        let input: ArrayRef = Arc::new(
            Decimal256Array::from(vec![Some(minimum), None])
                .with_precision_and_scale(76, s)
                .unwrap(),
        );
        compare(&input, DecimalOverflowPolicy::ReportError, true);
    }
    let input: ArrayRef = Arc::new(
        Decimal256Array::from(vec![Some(minimum)])
            .with_precision_and_scale(76, 2)
            .unwrap(),
    );
    assert_eq!(
        recipe(&input, DecimalOverflowPolicy::ReportError, true)
            .evaluate_row(EvaluatedArgument::Column(&input), 0, 0, &Control)
            .unwrap(),
        CastRowResult::Text(
            "--578960446186580977117854925043439539266349923328202820197287920039565648199.68"
                .into()
        )
    );
}
