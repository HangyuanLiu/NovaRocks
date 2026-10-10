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

//! Actual SQL-generated FoldRequests; the original test calculator loans the same arena route as FE.
use super::*;
#[test]
fn sql_fold_dependency_ds_scalar_actual_literal_three_arity_exact_source() {
    for (count, sql) in [
        (1, "SELECT ds_hll_count_distinct_state(7) AS folded"),
        (2, "SELECT ds_hll_count_distinct_state(7,10) AS folded"),
        (
            3,
            "SELECT ds_hll_count_distinct_state(7,10,'HLL_8') AS folded",
        ),
    ] {
        let observer = probe();
        let _source = compile(sql, &REAL, Some(observer.clone())).unwrap();
        let receipts = observer.receipts.lock().unwrap();
        let receipt = receipts
            .iter()
            .find(|r| {
                r.binding.as_ref().is_some_and(|b| {
                    b.function_id.as_str() == "builtin.scalar/ds_hll_count_distinct_state/v1"
                })
            })
            .expect("actual literal dependency must retain its original selected binding");
        let binding = receipt.binding.as_ref().unwrap();
        assert_eq!(binding.logical_argument_count, count);
        assert_eq!(receipt.arguments.len(), count);
        assert!(receipt.source_result_constraint.is_none());
        assert_eq!(
            receipt.source_constraint_origin,
            Some(novarocks_sql::binding::SqlResultConstraintOrigin::Unconstrained)
        );
        assert_eq!(
            receipt.request_result.data_type,
            arrow::datatypes::DataType::Binary
        );
        assert_eq!(
            binding.selected.result_type,
            FunctionResultType::Scalar(receipt.request_result.clone())
        );
        let Some(Outcome::Produced(value)) = &receipt.outcome else {
            panic!("original literal kernel must actually fold");
        };
        let a = value
            .pool()
            .array()
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .unwrap();
        let bytes = a.value(value.ordinal() as usize);
        assert_eq!(bytes[3], if count == 1 { 17 } else { 10 });
        assert_eq!((bytes[7] >> 2) & 3, if count == 3 { 2 } else { 1 });
        for arg in &receipt.arguments {
            assert_eq!(arg.value.value_type(), &arg.value_type);
        }
    }
}
