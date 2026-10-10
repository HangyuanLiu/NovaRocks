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
//! Original constant-source and full logical arity binding proof.
use super::super::legacy_struct_subfield_baseline_tests::{list, structure};
use super::ScalarDiffSpec;
use arrow::array::{Int32Array, StringArray};
use std::sync::Arc;

#[test]
fn array_struct_subfield_original_binding_constant_source_and_full_logical_arity() {
    use super::resolve_like_sql;
    use novarocks_functions::FunctionKind;
    let catalog = novarocks_functions::builtin::catalogue::builtin_engine_function_catalog();
    let a = list(
        structure(Arc::new(Int32Array::from(vec![Some(1), None])), None),
        vec![0, 1, 2],
        None,
    );
    for field in ["Chosen", "chosen"] {
        for count in [2, 3, 5] {
            let mut spec = ScalarDiffSpec::new("__array_struct_subfield")
                .column(a.clone())
                .constant_array(Arc::new(StringArray::from(vec![field])));
            for _ in 2..count {
                spec = spec.constant_array(Arc::new(Int32Array::from(vec![7])));
            }
            let bound = resolve_like_sql(
                catalog,
                "__array_struct_subfield",
                FunctionKind::Scalar,
                &spec.arguments,
            )
            .unwrap();
            assert_eq!(
                bound.function_id.as_str(),
                "builtin.scalar/__array_struct_subfield/v1"
            );
            assert_eq!(
                bound.selected.overload.as_str(),
                "builtin.scalar/__array_struct_subfield/dynamic-v1"
            );
            assert_eq!(bound.selected.argument_types.len(), count);
        }
    }
    // A field-name column without the original known-constant source fact is
    // not the admitted call, even when its observed values happen to agree.
    let no_constant = ScalarDiffSpec::new("__array_struct_subfield")
        .column(a)
        .column(Arc::new(StringArray::from(vec!["Chosen", "Chosen"])));
    assert!(
        resolve_like_sql(
            catalog,
            "__array_struct_subfield",
            FunctionKind::Scalar,
            &no_constant.arguments
        )
        .is_err()
    );
}
