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

//! Actual source journal/all-definition capability consumer; no replacement graph.
use super::numeric_unary_original_nonnull_sql_baseline_tests::sql_source;
use arrow::datatypes::DataType;
use novarocks_sql::compiler::SqlPhysicalEmissionMode;

use novarocks_sql::analyze_error::AnalyzeErrorKind;
use novarocks_sql::compiler::{
    SqlCompileControl, SqlCompileError, check_pure_call_definitions_observed,
};
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use std::sync::atomic::{AtomicUsize, Ordering};
#[test]
fn e08s1_late_original_actual_source_missing_aggregate_is_typed_named_unavailable() {
    let source = sql_source(
        "SELECT dict_merge(CAST(k AS VARCHAR), 1) FROM fixture",
        DataType::Int32,
        SqlPhysicalEmissionMode::OriginalNativeV1,
    );
    let error =
        check_pure_call_definitions_observed(&source, &SqlCompileControl::unbounded()).unwrap_err();
    let SqlCompileError::Analyze(error) = error else {
        panic!("typed support failure: {error}")
    };
    assert_eq!(error.kind(), AnalyzeErrorKind::UnavailableImplementation);
    assert!(error.message().contains("builtin.aggregate/dict_merge/"));
    // A completed physical source has no new AST span. Do not forge one.
    assert!(error.control_error().is_none());
    assert_eq!(
        source.emission_mode(),
        SqlPhysicalEmissionMode::OriginalNativeV1
    );
}
#[test]
fn e08s1_late_actual_source_same_journal_supported_canonical_calls_and_over_are_accepted() {
    for mode in [
        SqlPhysicalEmissionMode::OriginalNativeV1,
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
    ] {
        let source = sql_source(
            "SELECT reverse(CAST(k AS VARCHAR)), count(k) OVER () FROM fixture",
            DataType::Int32,
            mode,
        );
        check_pure_call_definitions_observed(&source, &SqlCompileControl::unbounded()).unwrap();
        assert_eq!(source.emission_mode(), mode);
    }
}
struct Refuse {
    cause: CompileControlError,
    calls: AtomicUsize,
}
impl PureCompileControl for Refuse {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        assert_eq!(
            self.calls.fetch_add(1, Ordering::Relaxed),
            0,
            "no tail callback"
        );
        Err(self.cause)
    }
}
#[test]
fn e08s1_late_actual_source_three_compile_causes_stop_before_first_definition() {
    let source = sql_source(
        "SELECT reverse(CAST(k AS VARCHAR)) FROM fixture",
        DataType::Int32,
        SqlPhysicalEmissionMode::OriginalNativeV1,
    );
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let control = Refuse {
            cause,
            calls: AtomicUsize::new(0),
        };
        let expected: SqlCompileError = cause.into();
        assert_eq!(
            check_pure_call_definitions_observed(&source, &control).unwrap_err(),
            expected
        );
        assert_eq!(control.calls.load(Ordering::Relaxed), 1);
    }
}
