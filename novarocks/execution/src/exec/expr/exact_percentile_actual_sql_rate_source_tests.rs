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

//! Actual original SQL request/binding source, prior to owner correction.
use super::numeric_unary_original_nonnull_sql_baseline_tests::sql_source;
use arrow::datatypes::DataType;
use novarocks_physical_plan::{
    FunctionArgumentType, NodeKind, PhysicalCallDefinition, PhysicalCallSite,
    StaticFunctionArgument,
};
use novarocks_sql::compiler::SqlPhysicalEmissionMode;
use novarocks_type_contract::FunctionValueType;
fn check(mode: SqlPhysicalEmissionMode) {
    for name in ["percentile_cont", "percentile_disc", "percentile_disc_lc"] {
        for rate in ["0", "1", "0.25", "0.5", "0.75", "CAST(0.5 AS DOUBLE)"] {
            let sql = format!("SELECT {name}(k,{rate}) AS observed FROM fixture");
            let source = sql_source(&sql, DataType::Int32, mode);
            let mut count = 0;
            for fragment in source.plan().fragments().values() {
                for node in fragment.nodes().values() {
                    let NodeKind::Aggregate { calls, .. } = &node.kind else {
                        continue;
                    };
                    for (ordinal, call) in calls.iter().enumerate() {
                        let site = PhysicalCallSite::Aggregate {
                            node: node.id,
                            call: ordinal as u32,
                        };
                        let request = fragment
                            .call_requests()
                            .get(PhysicalCallDefinition::Relational(site))
                            .unwrap();
                        assert_eq!(request.logical_argument_count, 2);
                        assert_eq!(call.binding.function.argument_types.len(), 2);
                        let StaticFunctionArgument::Value {
                            value_type: actual_rate,
                            ..
                        } = &request.arguments[1]
                        else {
                            panic!("original rate Value")
                        };
                        let FunctionArgumentType::Value(bound_rate) =
                            &call.binding.function.argument_types[1]
                        else {
                            panic!("selected rate Value")
                        };
                        assert_eq!(actual_rate, bound_rate);
                        // Record the actual authored type, rather than guessing
                        // it from ConstantValue or retagging a numeric literal.
                        eprintln!(
                            "original SQL rate source sql={sql:?} mode={mode:?} phase={:?} rate={actual_rate:?} state={:?} output={:?}",
                            call.binding.phase,
                            call.binding.intermediate_type,
                            call.binding.function.result_type
                        );
                        if !rate.starts_with("CAST") {
                            assert_ne!(actual_rate.data_type, DataType::Float64);
                        }
                        let FunctionArgumentType::Value(value) =
                            &call.binding.function.argument_types[0]
                        else {
                            panic!("value")
                        };
                        let mut expected = value.clone();
                        expected.nullable = true;
                        assert_eq!(call.binding.function.result_type, expected);
                        assert_eq!(
                            call.binding.intermediate_type,
                            FunctionValueType::new(DataType::Binary, true)
                        );
                        count += 1;
                    }
                }
            }
            assert!(count > 0, "actual aggregate source remains");
        }
    }
}
#[test]
fn exact_percentile_original_sql_rate_source() {
    check(SqlPhysicalEmissionMode::OriginalNativeV1);
}
#[test]
fn exact_percentile_candidate_sql_rate_source() {
    check(SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration);
}
