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
//! The original catalogue has no candidate-only static support check.
use arrow::datatypes::DataType;
use novarocks_functions::{FunctionArgument, FunctionValueType, PureCallLifecycle};
use novarocks_sql::compiler::{SqlCompileControl, builtin_sql_function_catalog};
#[test]
fn e08s1_percentile_original_static_source_all_three_full_bindings_remain_original() {
    let original = builtin_sql_function_catalog().snapshot();
    for name in ["percentile_cont", "percentile_disc", "percentile_disc_lc"] {
        let args = [
            FunctionArgument::Value {
                value_type: FunctionValueType::new(DataType::Decimal128(10, 2), false),
                constant: None,
            },
            FunctionArgument::Value {
                value_type: FunctionValueType::new(DataType::Decimal128(1, 1), false),
                constant: None,
            },
        ];
        let c = SqlCompileControl::unbounded();
        let binding = original
            .resolve_aggregate_binding(name, 2, &args, &c)
            .unwrap();
        assert_eq!(binding.logical_argument_count, 2);
        assert_eq!(binding.selected.argument_types.len(), 2);
        assert_eq!(
            binding
                .selected
                .aggregate
                .as_ref()
                .unwrap()
                .intermediate_type,
            FunctionValueType::new(DataType::Binary, true)
        );
        original
            .admit_bound_lifecycle_observed(&binding, PureCallLifecycle::Aggregate, &c)
            .unwrap();
    }
}
