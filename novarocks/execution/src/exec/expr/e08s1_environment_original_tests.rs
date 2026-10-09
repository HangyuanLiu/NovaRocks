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
//! Actual original SQL semantic sources; no new environment checker.
use super::numeric_unary_original_nonnull_sql_baseline_tests::sql_source_with_semantics;
use arrow::datatypes::DataType;
use novarocks_sql::compiler::{SqlAuthoredPhysicalPlan, SqlPhysicalEmissionMode};
use novarocks_type_contract::SemanticParameterValue;
pub(super) fn concat_source(mode: SqlPhysicalEmissionMode) -> SqlAuthoredPhysicalPlan {
    sql_source_with_semantics(
        "SELECT GROUP_CONCAT(CAST(k AS VARCHAR)) FROM fixture",
        DataType::Int64,
        mode,
        novarocks_sql::sql_mode::SqlSemanticSettings::default().with_group_concat_max_len(4096),
    )
}
pub(super) fn unix_source() -> SqlAuthoredPhysicalPlan {
    sql_source_with_semantics(
        "SELECT FROM_UNIXTIME(CAST(k AS BIGINT)) FROM fixture",
        DataType::Int64,
        SqlPhysicalEmissionMode::OriginalNativeV1,
        novarocks_sql::sql_mode::SqlSemanticSettings::default(),
    )
}
#[test]
fn e08s1_environment_original_concat_has_actual_lexical_sparse_values() {
    let source = concat_source(SqlPhysicalEmissionMode::OriginalNativeV1);
    let values = source.plan().parameters().entries();
    assert_eq!(
        values
            .values()
            .filter(|v| matches!(v, SemanticParameterValue::GroupConcatLegacy(_)))
            .count(),
        1
    );
    assert_eq!(
        values
            .values()
            .filter(|v| matches!(v, SemanticParameterValue::GroupConcatMaxLen(4096)))
            .count(),
        1
    );
}
#[test]
fn e08s1_environment_original_unixtime_has_no_statement_zone_author() {
    let source = unix_source();
    assert!(
        !source
            .plan()
            .parameters()
            .entries()
            .values()
            .any(|v| matches!(v, SemanticParameterValue::TimeZone(_)))
    );
    assert_eq!(
        source.emission_mode(),
        SqlPhysicalEmissionMode::OriginalNativeV1
    );
}
