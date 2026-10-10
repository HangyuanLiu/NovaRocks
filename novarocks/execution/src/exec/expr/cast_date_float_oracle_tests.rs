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

//! Permanent complete Date32 float profile differential and separate unchanged reverse mismatches.
use super::legacy_date_float_cast_baseline_tests::{actual, assert_literal, day, original};
use arrow::array::{Array, ArrayRef, Date32Array, Float32Array, Float64Array};
use arrow::datatypes::DataType;
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
        panic!("date float does not wait")
    }
}
fn recipe(
    source: DataType,
    target: DataType,
    source_nullable: bool,
    result_nullable: bool,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> PreparedCastRecipe {
    PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &FunctionValueType::new(source, source_nullable),
        &FunctionValueType::new(target, result_nullable),
        policy,
        allow,
        &Control,
    )
    .unwrap()
}
fn compare(
    input: &ArrayRef,
    target: DataType,
    source_nullable: bool,
    result_nullable: bool,
    policy: DecimalOverflowPolicy,
    allow: bool,
) {
    let r = recipe(
        DataType::Date32,
        target.clone(),
        source_nullable,
        result_nullable,
        policy,
        allow,
    );
    let old = actual(input, target.clone(), policy, allow).unwrap();
    for row in 0..input.len() {
        let got = r
            .evaluate_row(EvaluatedArgument::Column(input), row, row, &Control)
            .unwrap();
        if old.is_null(row) {
            assert_eq!(got, CastRowResult::Null);
            continue;
        }
        match got {
            CastRowResult::Float32(v) => assert_eq!(
                v.to_bits(),
                old.as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap()
                    .value(row)
                    .to_bits()
            ),
            CastRowResult::Float64(v) => assert_eq!(
                v.to_bits(),
                old.as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(row)
                    .to_bits()
            ),
            other => panic!("wrong exact float output {other:?}"),
        }
    }
}
fn complete_profile(target: DataType) {
    // Sample across the whole valid chrono span, including both arithmetic overflow classes below.
    // This fixture is evidence, not a narrower admitted source type or runtime filter.
    let mut values = vec![
        Some(0),
        Some(-1),
        Some(day(0, 1, 1)),
        Some(day(-1, 12, 31)),
        Some(day(1900, 2, 28)),
        Some(day(2000, 2, 29)),
        Some(day(2024, 2, 29)),
        Some(day(214748, 12, 31)),
        Some(day(-214748, 12, 31)),
    ];
    for year in (-214748..=214748).step_by(4093) {
        for (month, date) in [(1, 1), (6, 30), (12, 31)] {
            values.push(Some(day(year, month, date)));
        }
    }
    for source_nullable in [false, true] {
        let mut rows = values.clone();
        if source_nullable {
            rows.push(None);
        }
        let input: ArrayRef = Arc::new(Date32Array::from(rows));
        for result_nullable in [false, true] {
            if source_nullable && !result_nullable {
                continue;
            }
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for allow in [false, true] {
                    compare(
                        &input,
                        target.clone(),
                        source_nullable,
                        result_nullable,
                        policy,
                        allow,
                    );
                    compare(
                        &input.slice(1, input.len() - 1),
                        target.clone(),
                        source_nullable,
                        result_nullable,
                        policy,
                        allow,
                    );
                    compare(
                        &input.slice(0, 0),
                        target.clone(),
                        source_nullable,
                        result_nullable,
                        policy,
                        allow,
                    );
                    let r = recipe(
                        DataType::Date32,
                        target.clone(),
                        source_nullable,
                        result_nullable,
                        policy,
                        allow,
                    );
                    let constant = input.slice(1, 1);
                    for (ordinal, row) in Selection::try_sparse(input.len(), &[0, 3, 7])
                        .unwrap()
                        .iter()
                        .enumerate()
                    {
                        let expected = match target {
                            DataType::Float32 => CastRowResult::Float32(19691231_i32 as f32),
                            _ => CastRowResult::Float64(19691231.0),
                        };
                        assert_eq!(
                            r.evaluate_row(
                                EvaluatedArgument::Scalar(&constant),
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
    let hidden: ArrayRef = Arc::new(Date32Array::new(
        vec![i32::MAX, 0].into(),
        Some(arrow_buffer::NullBuffer::from(vec![false, true])),
    ));
    compare(
        &hidden,
        target.clone(),
        true,
        true,
        DecimalOverflowPolicy::ReportError,
        true,
    );
    let input: ArrayRef = Arc::new(Date32Array::from(vec![
        i32::MAX,
        0,
        i32::MIN,
        day(2024, 2, 29),
    ]));
    let r = recipe(
        DataType::Date32,
        target.clone(),
        false,
        true,
        DecimalOverflowPolicy::OutputNull,
        false,
    );
    // Genuine demand skips the first original panic; selected error uses selected ordinal, not logical row.
    for (ordinal, row) in Selection::try_sparse(4, &[1, 3])
        .unwrap()
        .iter()
        .enumerate()
    {
        assert!(matches!(
            r.evaluate_row(EvaluatedArgument::Column(&input), ordinal, row, &Control)
                .unwrap(),
            CastRowResult::Float32(_) | CastRowResult::Float64(_)
        ));
    }
    let bad = input.slice(2, 1);
    let message = original(&bad, target.clone()).unwrap_err();
    match r
        .evaluate_row(EvaluatedArgument::Column(&input), 7, 2, &Control)
        .unwrap()
    {
        CastRowResult::RowError(e) => {
            assert_eq!(e.selected_ordinal(), 7);
            assert_eq!(e.message(), message);
        }
        other => panic!("expected preserved original Date32 error, got {other:?}"),
    }
    // All i32 Date32 carriers remain admitted; original panic/wrapping is neither NULL nor row-error translated.
    for days in [
        i32::MAX,
        day(262142, 12, 31),
        day(-262143, 1, 1),
        day(214749, 1, 1),
        day(-214749, 1, 1),
    ] {
        let input: ArrayRef = Arc::new(Date32Array::from(vec![days]));
        let old = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            original(&input, target.clone())
        }));
        let new = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            r.evaluate_row(EvaluatedArgument::Column(&input), 0, 0, &Control)
        }));
        if cfg!(debug_assertions) {
            assert!(old.is_err());
            assert!(new.is_err());
        } else {
            match old.unwrap() {
                Ok(array) => match new.unwrap().unwrap() {
                    CastRowResult::Float32(v) => assert_eq!(
                        v.to_bits(),
                        array
                            .as_any()
                            .downcast_ref::<Float32Array>()
                            .unwrap()
                            .value(0)
                            .to_bits()
                    ),
                    CastRowResult::Float64(v) => assert_eq!(
                        v.to_bits(),
                        array
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .unwrap()
                            .value(0)
                            .to_bits()
                    ),
                    other => panic!("wrong wrapped result {other:?}"),
                },
                Err(message) => match new.unwrap().unwrap() {
                    CastRowResult::RowError(e) => assert_eq!(e.message(), message),
                    other => panic!("wrong original invalid result {other:?}"),
                },
            }
        }
    }
}
#[test]
fn date_float_oracle_complete_date32_float32_profile() {
    complete_profile(DataType::Float32);
}
#[test]
fn date_float_oracle_complete_date32_float64_profile() {
    complete_profile(DataType::Float64);
}
