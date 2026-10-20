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

//! Actual original source publication before all-definition capability admission.
use super::sql_test_source::sql_source;
use arrow::datatypes::DataType;

#[test]
fn e08s1_late_original_full_source_has_scalar_and_aggregate_over_definitions() {
    let source = sql_source(
        "SELECT reverse(CAST(k AS VARCHAR)), count(k) OVER () FROM fixture",
        DataType::Int32,
    );
    assert!(!source.plan().fragments().is_empty());
}
