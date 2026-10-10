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
//! Same original current-root binding is checked even when LogicalOnly has no fold walk.
use super::e08s1_logical_environment_original_tests::logical;
use super::sql_scalar_presence_original_tests::{SERIAL, fold_count, reset};
use novarocks_sql::{
    analyze_error::AnalyzeErrorKind,
    compiler::{SqlCompileError, SqlPhysicalEmissionMode},
};
#[test]
fn e08s1_logical_environment_exact_absent_source_is_named_with_original_span() {
    let _serial = SERIAL.lock().unwrap();
    for (sql, name) in [
        (
            "SELECT GROUP_CONCAT(REVERSE('abc'))",
            "builtin.aggregate/group_concat/",
        ),
        (
            "SELECT FROM_UNIXTIME(CAST(ABS(CAST(1 AS BIGINT)) AS BIGINT))",
            "builtin.scalar/from_unixtime/",
        ),
    ] {
        reset();
        let error = logical(
            sql,
            SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
            None,
        )
        .unwrap_err();
        let SqlCompileError::Analyze(error) = error else {
            panic!("original AST binding refusal: {error:?}")
        };
        assert_eq!(error.kind(), AnalyzeErrorKind::UnavailableImplementation);
        assert!(error.message().contains(name));
        assert!(
            error.span().is_some(),
            "the direct actual AST root supplies its parser span"
        );
        assert_eq!(fold_count(), 0);
    }
}
#[test]
fn e08s1_logical_environment_exact_authored_raw_limit_keeps_original_logical_success() {
    let _serial = SERIAL.lock().unwrap();
    for raw in [-1, 4096] {
        reset();
        logical(
            "SELECT GROUP_CONCAT(REVERSE('abc'))",
            SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
            Some(raw),
        )
        .unwrap();
        assert_eq!(fold_count(), 0);
    }
}
