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
use super::sql_test_source::sql_source_with_semantics;
use arrow::datatypes::DataType;
use novarocks_sql::compiler::SqlAuthoredPhysicalPlan;
use novarocks_type_contract::SemanticParameterValue;
pub(super) fn concat_source() -> SqlAuthoredPhysicalPlan {
    sql_source_with_semantics(
        "SELECT GROUP_CONCAT(CAST(k AS VARCHAR)) FROM fixture",
        DataType::Int64,
        novarocks_sql::sql_mode::SqlSemanticSettings::default().with_group_concat_max_len(4096),
    )
}
pub(super) fn unix_source()
-> Result<SqlAuthoredPhysicalPlan, novarocks_sql::compiler::SqlCompileProgressError> {
    super::sql_test_source::try_sql_source_with_declared_fields(
        "SELECT FROM_UNIXTIME(CAST(k AS BIGINT)) FROM fixture",
        &[arrow::datatypes::Field::new("k", DataType::Int64, false)],
    )
}
#[test]
fn e08s1_environment_original_concat_has_actual_lexical_sparse_values() {
    let source = concat_source();
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
