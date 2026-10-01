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

//! SQL catalogue integration tests for the Functions-owned builtin registry.

#[cfg(test)]
mod bitmap_base64_type_tests {
    use arrow::datatypes::DataType;
    use novarocks_functions::{
        FunctionArgument, FunctionBindingRequest, FunctionKind, FunctionResultType,
        FunctionValueType,
    };

    #[test]
    fn bitmap_base64_freezes_text_result_and_binary_input() {
        let catalog = crate::functions::build_builtin_engine_function_catalog().unwrap();
        for (ty, nullable) in [
            (DataType::Binary, false),
            (DataType::Binary, true),
            (DataType::Null, true),
        ] {
            let arguments = [FunctionArgument::Value {
                value_type: FunctionValueType::new(ty, nullable),
                constant: None,
            }];
            let request = FunctionBindingRequest {
                expected_result_type: None,
                arguments: &arguments,
                logical_argument_count: 1,
            };
            let binding = catalog
                .resolve_bound_user(
                    "bitmap_to_base64",
                    FunctionKind::Scalar,
                    request,
                    &crate::compiler::SqlCompileControl::unbounded(),
                )
                .unwrap();
            assert_eq!(
                binding.selected.result_type,
                FunctionResultType::Scalar(FunctionValueType::new(DataType::Utf8, true))
            );
            assert_eq!(
                binding.selected.argument_types,
                vec![novarocks_functions::FunctionArgumentType::Value(
                    FunctionValueType::new(DataType::Binary, nullable)
                )]
                .into_boxed_slice()
            );
            // Exact validation consumes already-coerced expressions, as SQL does.
            let coerced = [FunctionArgument::Value {
                value_type: FunctionValueType::new(DataType::Binary, nullable),
                constant: None,
            }];
            catalog
                .validate_bound(
                    &binding,
                    FunctionBindingRequest {
                        expected_result_type: None,
                        arguments: &coerced,
                        logical_argument_count: 1,
                    },
                    &crate::compiler::SqlCompileControl::unbounded(),
                )
                .unwrap();
        }
        for types in [
            vec![],
            vec![DataType::Int64],
            vec![DataType::Utf8],
            vec![DataType::Binary, DataType::Binary],
        ] {
            let arguments = types
                .into_iter()
                .map(|ty| FunctionArgument::Value {
                    value_type: FunctionValueType::new(ty, true),
                    constant: None,
                })
                .collect::<Vec<_>>();
            assert!(
                catalog
                    .resolve_bound_user(
                        "bitmap_to_base64",
                        FunctionKind::Scalar,
                        FunctionBindingRequest {
                            expected_result_type: None,
                            arguments: &arguments,
                            logical_argument_count: arguments.len()
                        },
                        &crate::compiler::SqlCompileControl::unbounded()
                    )
                    .is_err()
            );
        }
        // Other bitmap-producing declarations still freeze binary results.
        let resolved = crate::functions::resolver::resolve_scalar_function_signature(
            "bitmap_to_binary",
            &[DataType::Binary],
        )
        .unwrap();
        assert_eq!(resolved.return_type, DataType::Binary);
    }
}
