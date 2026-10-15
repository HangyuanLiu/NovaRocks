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

//! Pure source catalogue authors for the existing Strong scalar fixtures.
//! Requests are explicit original inputs, independent of mutable wire children.
use super::fixture_sources as sources;
use novarocks_functions::{
    FunctionArgument, FunctionArgumentType, FunctionBindingRequest, FunctionKind,
    FunctionResultType, ResolvedFunctionBinding,
};
use novarocks_proto_models::{call_request as request, expr, physical_package_v2 as binding};
use novarocks_type_contract::{CompilePhase, FunctionValueType, PureCompileControl};
struct Control;
impl PureCompileControl for Control {
    fn checkpoint(
        &self,
        _: CompilePhase,
        _: u32,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        Ok(())
    }
}
pub(super) struct ScalarSource {
    selected: ResolvedFunctionBinding,
    arguments: Vec<FunctionArgument>,
    references: Vec<Option<request::ConstantReference>>,
}
fn type_id(ty: &FunctionValueType) -> u32 {
    sources::type_desc_with_nullable(ty, ty.nullable())
        .value_binding_id
        .unwrap()
}
fn lambda(params: &[FunctionValueType], result: &FunctionValueType) -> request::LambdaArgumentType {
    request::LambdaArgumentType {
        parameter_value_type_ids: params.iter().map(type_id).collect(),
        result_value_type_id: Some(type_id(result)),
    }
}
impl ScalarSource {
    pub(super) fn new(name: &str, arguments: &[FunctionArgument]) -> Self {
        let catalog = novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap();
        let selected = catalog
            .resolve_bound_user(
                name,
                FunctionKind::Scalar,
                FunctionBindingRequest {
                    arguments,
                    logical_argument_count: arguments.len(),
                    expected_result_type: None,
                },
                &Control,
            )
            .unwrap();
        Self {
            selected,
            arguments: arguments.to_vec(),
            references: vec![None; arguments.len()],
        }
    }
    pub(super) fn with_literals(
        name: &str,
        originals: &[(FunctionValueType, expr::LiteralExpr)],
    ) -> Self {
        let mut arguments = Vec::new();
        let mut references = Vec::new();
        for (ty, literal) in originals {
            let (constant, reference) = sources::original_literal_value(ty, literal);
            arguments.push(FunctionArgument::Value {
                value_type: ty.clone(),
                constant: Some(constant),
            });
            references.push(Some(reference));
        }
        let mut source = Self::new(name, &arguments);
        source.references = references;
        source
    }
    pub(super) fn result(&self) -> &FunctionValueType {
        let FunctionResultType::Scalar(result) = &self.selected.selected.result_type else {
            panic!("scalar result")
        };
        result
    }
    pub(super) fn attach(&self, expression: &mut expr::Expr, definition: u32) {
        let Some(expr::expr::Kind::FunctionCall(call)) = &mut expression.kind else {
            panic!("fixture scalar source")
        };
        let FunctionResultType::Scalar(result) = &self.selected.selected.result_type else {
            panic!("fixture scalar result")
        };
        call.frozen_binding = Some(binding::FunctionBindingDefinition {
            id: definition,
            function_id: self.selected.function_id.as_str().into(),
            overload_id: self.selected.selected.overload.as_str().into(),
            kind: binding::FunctionKind::Scalar as i32,
            arguments: self
                .selected
                .selected
                .argument_types
                .iter()
                .map(|argument| binding::FunctionArgumentType {
                    kind: Some(match argument {
                        FunctionArgumentType::Value(ty) => {
                            binding::function_argument_type::Kind::ValueTypeId(type_id(ty))
                        }
                        FunctionArgumentType::Lambda {
                            parameter_types,
                            result_type,
                        } => binding::function_argument_type::Kind::Lambda(lambda(
                            parameter_types,
                            result_type,
                        )),
                    }),
                })
                .collect(),
            result: Some(
                binding::function_binding_definition::Result::ScalarValueTypeId(type_id(result)),
            ),
        });
        call.original_request_id = Some(definition);
        expression.original_definition_id = Some(definition);
    }
    fn request(&self, definition: u32) -> request::OriginalCallRequest {
        request::OriginalCallRequest {
            request_id: Some(definition),
            definition: Some(request::CallRequestDefinition {
                kind: Some(
                    request::call_request_definition::Kind::ExpressionDefinitionId(definition),
                ),
            }),
            arguments: self
                .arguments
                .iter()
                .zip(&self.references)
                .map(|(argument, reference)| request::OriginalFunctionArgument {
                    kind: Some(match argument {
                        FunctionArgument::Value {
                            value_type,
                            constant: _,
                        } => request::original_function_argument::Kind::Value(
                            request::OriginalValueArgument {
                                value_type_id: Some(type_id(value_type)),
                                constant: reference.clone(),
                            },
                        ),
                        FunctionArgument::Lambda {
                            parameter_types,
                            result_type,
                        } => request::original_function_argument::Kind::Lambda(lambda(
                            parameter_types,
                            result_type,
                        )),
                    }),
                })
                .collect(),
            logical_argument_count: Some(u32::try_from(self.arguments.len()).unwrap()),
            expected_result_value_type_id: None,
            constant_policy: Some(request::SourceConstantPolicy {
                max_rows: Some(1),
                max_array_nodes: Some(1),
                max_logical_elements: Some(1),
                max_retained_buffer_bytes: Some(4096),
                max_type_depth: Some(1),
                max_type_nodes: Some(1),
                max_dictionary_depth: Some(1),
                max_metadata_bytes: Some(16),
                max_library_validation_work: Some(65536),
                max_library_validation_bytes: Some(65536),
            }),
        }
    }
    pub(super) fn lower_error(
        &self,
        expression: &expr::Expr,
        slots: &[u32],
        definition: u32,
    ) -> String {
        let mut arena = novarocks_execution::exec::expr::ExprArena::default();
        let layout = super::tests::layout_for_slots(slots);
        sources::with_original_requests(&[self.request(definition)], |decoder| {
            super::decode_expr_at_with_types(
                expression,
                novarocks_proto_codec::FieldPath::root("expr"),
                &mut arena,
                &layout,
                decoder,
            )
        })
        .unwrap_err()
        .to_string()
    }
    pub(super) fn lower(
        &self,
        expression: &expr::Expr,
        slots: &[u32],
        definition: u32,
    ) -> (
        novarocks_execution::exec::expr::ExprArena,
        novarocks_execution::exec::expr::ExprId,
    ) {
        let mut arena = novarocks_execution::exec::expr::ExprArena::default();
        let layout = super::tests::layout_for_slots(slots);
        let id = sources::with_original_requests(&[self.request(definition)], |decoder| {
            super::decode_expr_at_with_types(
                expression,
                novarocks_proto_codec::FieldPath::root("expr"),
                &mut arena,
                &layout,
                decoder,
            )
        })
        .unwrap();
        (arena, id)
    }
}
