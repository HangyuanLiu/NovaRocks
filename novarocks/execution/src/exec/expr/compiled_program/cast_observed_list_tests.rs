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

//! Three independently RED observed FVT profiles through actual LocalCompiler/Frame.
use super::*;
use crate::exec::expr::legacy_observed_list_cast_baseline_tests::{
    actual, input, modes, same_values, target_type,
};
use arrow::array::UInt64Array;
fn batch(program: &LocalProgram, source: ArrayRef) -> RecordBatch {
    let len = source.len();
    RecordBatch::try_new(
        program.graph().nodes()[1].output_layout().schema().clone(),
        vec![
            source,
            Arc::new(Int64Array::from(vec![42; len])),
            Arc::new(BooleanArray::from(vec![true; len])),
        ],
    )
    .unwrap()
}
fn compare(null: bool, id: Option<&str>) {
    for (policy, allow) in modes() {
        let full = input(null);
        let target = target_type(null, id);
        let program = compiled(
            FunctionValueType::new(full.data_type().clone(), true),
            FunctionValueType::new(target.clone(), true),
            Source::Column,
            Wrap::Bare,
            policy,
            allow,
        );
        for source in [full.clone(), full.slice(1, 3), full.slice(1, 0)] {
            let b = batch(&program, source.clone());
            let old = actual(source.clone(), target.clone(), policy, allow).unwrap();
            for rows in [
                (0..source.len()).collect::<Vec<_>>(),
                (0..source.len()).filter(|r| r % 2 == 0).collect(),
                Vec::new(),
            ] {
                let selection = Selection::try_sparse(source.len(), &rows).unwrap();
                let out = instance(&program)
                    .evaluate(&b, selection, &Control)
                    .unwrap();
                assert_eq!(out.selection(), selection);
                assert_eq!(out.values().data_type(), &target);
                assert!(out.errors().is_empty());
                let indices = UInt64Array::from(
                    rows.iter()
                        .map(|r| u64::try_from(*r).unwrap())
                        .collect::<Vec<_>>(),
                );
                let expected = arrow::compute::take(old.as_ref(), &indices, None).unwrap();
                same_values(out.values(), &expected);
            }
        }
    }
}
#[test]
fn observed_list_cast_actual_compiler_null_child_to_int32_exact_profile() {
    compare(true, None);
}
#[test]
fn observed_list_cast_actual_compiler_utf8_to_field_id7_exact_profile() {
    compare(false, Some("7"));
}
#[test]
fn observed_list_cast_actual_compiler_utf8_to_field_id6_exact_profile() {
    compare(false, Some("6"));
}
