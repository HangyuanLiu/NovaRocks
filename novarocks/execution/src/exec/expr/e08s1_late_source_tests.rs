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
use super::sql_test_source::sql_source;
use arrow::datatypes::DataType;

use novarocks_sql::compiler::{
    SqlCompileControl, SqlCompileError, check_pure_call_definitions_observed,
};
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn e08s1_late_actual_source_same_journal_supported_canonical_calls_and_over_are_accepted() {
    {
        let source = sql_source(
            "SELECT reverse(CAST(k AS VARCHAR)), count(k) OVER () FROM fixture",
            DataType::Int32,
        );
        check_pure_call_definitions_observed(&source, &SqlCompileControl::unbounded()).unwrap();
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
        Err(self.cause.clone())
    }
}
#[test]
fn e08s1_late_actual_source_three_compile_causes_stop_before_first_definition() {
    let source = sql_source(
        "SELECT reverse(CAST(k AS VARCHAR)) FROM fixture",
        DataType::Int32,
    );
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let control = Refuse {
            cause: cause.clone(),
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
