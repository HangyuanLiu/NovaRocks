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
//! Actual LocalCompiler recipe + Frame equality against the original arena TIME shell.
use super::*;
use crate::exec::expr::cast_calendar_time_oracle_tests::corpus as calendar_corpus;
use arrow::datatypes::TimeUnit;
fn calendar_check(dtype: DataType) {
    for allow in [false, true] {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            let program = compiled(
                FunctionValueType::new(dtype.clone(), true),
                FunctionValueType::new(DataType::Time64(TimeUnit::Microsecond), true),
                Source::Column,
                Wrap::Bare,
                policy,
                allow,
            );
            for source in [
                calendar_corpus(&dtype),
                calendar_corpus(&dtype).slice(1, 6),
                calendar_corpus(&dtype).slice(0, 0),
            ] {
                let expected = values(&actual(source.clone(), allow, policy, false).unwrap());
                let batch = batch(&program, source.clone());
                for rows in [
                    (0..source.len()).collect::<Vec<_>>(),
                    (0..source.len()).filter(|r| r % 3 == 0).collect(),
                ] {
                    let selection = Selection::try_sparse(source.len(), &rows).unwrap();
                    let output = instance(&program)
                        .evaluate(&batch, selection, &Control)
                        .unwrap();
                    assert!(output.errors().is_empty());
                    assert_eq!(
                        values(output.values()),
                        rows.iter().map(|r| expected[*r]).collect::<Vec<_>>()
                    );
                }
            }
        }
    }
}
fn calendar_timestamp_check(unit: TimeUnit) {
    for zone in [
        None,
        Some("UTC".into()),
        Some("Asia/Shanghai".into()),
        Some("opaque-original-zone".into()),
    ] {
        calendar_check(DataType::Timestamp(unit.clone(), zone));
    }
}
#[test]
fn calendar_time_actual_compiler_date() {
    calendar_check(DataType::Date32);
}
#[test]
fn calendar_time_actual_compiler_second() {
    calendar_timestamp_check(TimeUnit::Second);
}
#[test]
fn calendar_time_actual_compiler_millisecond() {
    calendar_timestamp_check(TimeUnit::Millisecond);
}
#[test]
fn calendar_time_actual_compiler_microsecond() {
    calendar_timestamp_check(TimeUnit::Microsecond);
}
#[test]
fn calendar_time_actual_compiler_nanosecond() {
    calendar_timestamp_check(TimeUnit::Nanosecond);
}
#[test]
fn calendar_time_actual_compiler_every_cause_stops_at_actual_callback_and_latches() {
    for dtype in [
        DataType::Date32,
        DataType::Timestamp(TimeUnit::Microsecond, None),
    ] {
        let program = compiled(
            FunctionValueType::new(dtype.clone(), true),
            FunctionValueType::new(DataType::Time64(TimeUnit::Microsecond), true),
            Source::Column,
            Wrap::Bare,
            DecimalOverflowPolicy::ReportError,
            true,
        );
        let batch = batch(&program, calendar_corpus(&dtype));
        let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
        assert!(
            instance(&program)
                .evaluate(&batch, Selection::all(batch.num_rows()), &recorder)
                .is_ok()
        );
        let trace = recorder.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        assert!(trace.iter().all(|n| *n <= 256));
        for at in 1..=trace.len() {
            for cause in [
                KernelFailure::Cancelled,
                KernelFailure::DeadlineExceeded,
                KernelFailure::ResourceExhausted,
                KernelFailure::InvalidProgram(KernelDiagnostic::new("calendar invalid")),
                KernelFailure::Internal(KernelDiagnostic::new("calendar internal")),
                KernelFailure::Operational(KernelDiagnostic::new("calendar host")),
                KernelFailure::InstanceFailed,
            ] {
                let control = CallbackControl::new(cause.clone(), at);
                let mut runtime = instance(&program);
                assert_eq!(
                    runtime
                        .evaluate(&batch, Selection::all(batch.num_rows()), &control)
                        .unwrap_err(),
                    cause
                );
                assert_eq!(control.trace.lock().unwrap().clone(), trace[..at]);
                let saved = control.trace.lock().unwrap().clone();
                assert_eq!(
                    runtime
                        .evaluate(&batch, Selection::all(batch.num_rows()), &control)
                        .unwrap_err(),
                    KernelFailure::InstanceFailed
                );
                assert_eq!(control.trace.lock().unwrap().clone(), saved);
            }
        }
    }
}
#[test]
fn calendar_time_exact_successful_null_fact_retains_nonnullable_date_and_nano_domains() {
    use novarocks_functions::{CastOperation, CastPrepareError, PreparedCastRecipe};
    for dtype in [
        DataType::Date32,
        DataType::Timestamp(TimeUnit::Nanosecond, None),
    ] {
        assert!(!novarocks_functions::carrier_cast_can_produce_null(
            &dtype,
            &DataType::Time64(TimeUnit::Microsecond),
            false
        ));
        assert!(
            PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &FunctionValueType::new(dtype, false),
                &FunctionValueType::new(DataType::Time64(TimeUnit::Microsecond), false),
                DecimalOverflowPolicy::ReportError,
                false,
                &Control
            )
            .is_ok()
        );
    }
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
    ] {
        let dtype = DataType::Timestamp(unit, None);
        assert!(novarocks_functions::carrier_cast_can_produce_null(
            &dtype,
            &DataType::Time64(TimeUnit::Microsecond),
            false
        ));
        assert_eq!(
            PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &FunctionValueType::new(dtype, false),
                &FunctionValueType::new(DataType::Time64(TimeUnit::Microsecond), false),
                DecimalOverflowPolicy::ReportError,
                false,
                &Control
            ),
            Err(CastPrepareError::TypeMismatch)
        );
    }
}
