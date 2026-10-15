// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Original aggregate requests for Strong Native decode fixtures.
//! Selection comes from the catalogue's original full request, independently
//! of mutable wire signatures and state-phase invocation arguments.
use super::fixture_sources::{self as sources, Declaration};
use arrow::{
    array::{Array, StringArray},
    datatypes::{DataType, Field},
};
use novarocks_functions::{
    ConstantPolicy, ConstantPool, ConstantValue, FunctionArgument, FunctionArgumentType,
    FunctionBindingRequest, FunctionKind, FunctionResultType, FunctionValueType,
    ResolvedFunctionBinding,
};
use novarocks_proto_models::{
    call_request as wire, common, expr, physical_semantics_v2 as semantics, plan,
};
use novarocks_type_contract::{CompilePhase, PureCompileControl};
use std::sync::Arc;
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
#[derive(Clone)]
pub(crate) struct AggregateSource {
    pub(crate) selected: ResolvedFunctionBinding,
    arguments: Vec<FunctionArgument>,
    references: Vec<Option<wire::ConstantReference>>,
    logical_count: usize,
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 1,
        max_array_nodes: 1,
        max_logical_elements: 1,
        max_retained_buffer_bytes: 4096,
        max_type_depth: 1,
        max_type_nodes: 1,
        max_dictionary_depth: 1,
        max_metadata_bytes: 16,
        max_library_validation_work: 65536,
        max_library_validation_bytes: 65536,
    }
}
impl AggregateSource {
    pub(crate) fn new(name: &str, logical: &[DataType], update: &[DataType]) -> Self {
        Self::with_literals(name, logical, update, &[])
    }
    pub(crate) fn with_literals(
        name: &str,
        logical: &[DataType],
        update: &[DataType],
        literals: &[(usize, common::literal_value::Value)],
    ) -> Self {
        assert!(update.len() >= logical.len());
        assert_eq!(&update[..logical.len()], logical);
        let mut arguments = Vec::new();
        let mut references = Vec::new();
        for (index, ty) in update.iter().enumerate() {
            if let Some((_, value)) = literals.iter().find(|(ordinal, _)| *ordinal == index) {
                let ty = ty.declared().with_nullable(false).unwrap();
                let data = match value {
                    common::literal_value::Value::StringValue(value) => {
                        StringArray::from(vec![value.as_str()]).to_data()
                    }
                    common::literal_value::Value::BoolValue(value) => {
                        arrow::array::BooleanArray::from(vec![*value]).to_data()
                    }
                    _ => panic!("the fixture supplies its exact original literal constructor"),
                };
                let pool = ConstantPool::try_new(
                    Arc::new(Field::new(
                        "request literal",
                        ty.data_type().clone(),
                        ty.nullable(),
                    )),
                    ty.clone(),
                    data,
                    policy(),
                    CompilePhase::Validate,
                    &Control,
                )
                .unwrap();
                arguments.push(FunctionArgument::Value {
                    value_type: ty.clone(),
                    constant: Some(pool.value(0).unwrap()),
                });
                references.push(Some(sources::constant_reference(
                    &ty,
                    &expr::LiteralExpr {
                        value: Some(common::LiteralValue {
                            value: Some(value.clone()),
                        }),
                    },
                )));
            } else {
                arguments.push(FunctionArgument::Value {
                    value_type: ty.declared(),
                    constant: None,
                });
                references.push(None);
            }
        }
        let catalog = novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap();
        let request = FunctionBindingRequest {
            arguments: &arguments,
            logical_argument_count: logical.len(),
            expected_result_type: None,
        };
        let selected = catalog
            .resolve_bound_user(name, FunctionKind::Aggregate, request, &Control)
            .unwrap();
        catalog
            .validate_bound(&selected, request, &Control)
            .unwrap();
        Self {
            selected,
            arguments,
            references,
            logical_count: logical.len(),
        }
    }
    pub(crate) fn writer_call(
        &self,
        site: novarocks_physical_plan::PhysicalCallSite,
        phase: novarocks_functions::AggregateKernelPhase,
        identity: u32,
    ) -> novarocks_proto_models::physical_semantics_v2::FrozenCall {
        use novarocks_functions::{
            AggregatePreparationOptions, CallArgumentUses, CallEffectInput, PureCallPreparation,
            ScopedExpressionEffects,
        };
        use novarocks_type_contract::{
            CallProofScope, CompileCheckpoints, DecimalOverflowPolicy, EvaluationDemand,
            EvaluationDomainId, ExpressionEffectContext, ExpressionUseId, SemanticParameters,
        };
        let call = ExpressionEffectContext {
            use_id: ExpressionUseId::new(identity.checked_mul(2).unwrap()),
            domain: EvaluationDomainId::new(identity.checked_mul(2).unwrap()),
            demand: EvaluationDemand::Value,
        };
        let input = ExpressionEffectContext {
            use_id: ExpressionUseId::new(identity.checked_mul(2).unwrap().checked_add(1).unwrap()),
            domain: EvaluationDomainId::new(
                identity.checked_mul(2).unwrap().checked_add(1).unwrap(),
            ),
            demand: EvaluationDemand::Value,
        };
        let parameters = SemanticParameters::try_new([]).unwrap();
        let selected = Arc::new(self.selected.selected.clone());
        let catalog = novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap();
        let uses = [Some(input.use_id)];
        let (argument_uses, state_input_type) = if phase.consumes_logical_arguments() {
            (CallArgumentUses::SelectedChannels(&uses), None)
        } else {
            (
                CallArgumentUses::AggregateMerge {
                    phase,
                    state_context: input,
                    state_input_type: self.intermediate(),
                },
                Some(self.intermediate().clone()),
            )
        };
        let authored = catalog
            .prepare_fresh_selected(
                CallEffectInput {
                    context: call,
                    argument_uses,
                    function_id: &self.selected.function_id,
                    kind: FunctionKind::Aggregate,
                    selected: &selected,
                    request: FunctionBindingRequest {
                        arguments: &self.arguments,
                        logical_argument_count: self.logical_count,
                        expected_result_type: None,
                    },
                    environment: &[],
                    parameters: &parameters,
                    decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                    proof_scope: CallProofScope::Domain(call.domain),
                },
                selected.clone(),
                PureCallPreparation::Aggregate {
                    arguments: ScopedExpressionEffects::primitive(
                        call,
                        ScopedExpressionEffects::pure_value(input)
                            .for_use(input)
                            .unwrap(),
                    ),
                    options: AggregatePreparationOptions {
                        state_interpretation: None,
                        phase,
                        distinct: false,
                        order_keys: Arc::from([]),
                        state_input_type,
                    },
                },
                &Control,
            )
            .unwrap();
        let frozen = novarocks_physical_plan::FrozenPhysicalCall {
            site,
            context: call,
            materialized_writer_input: Some(input),
            effects: authored.call_contract().effects().clone(),
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            temporal_source: None,
            regexp_count_pattern_source: None,
            to_base64_byte_source: None,
        };
        let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Encode).unwrap();
        let wire = novarocks_plan_codec::physical_semantics_v2::encode_frozen_writer_call_with_creator_in::<std::convert::Infallible>(&frozen, None, &mut work).unwrap();
        work.finish().unwrap();
        wire
    }
    pub(crate) fn roots(&self) -> Vec<FunctionValueType> {
        let mut roots = Vec::new();
        for arg in &self.arguments {
            let FunctionArgument::Value { value_type, .. } = arg else {
                panic!("value source")
            };
            roots.push(value_type.clone());
        }
        for arg in &self.selected.selected.argument_types {
            let FunctionArgumentType::Value(ty) = arg else {
                panic!("value selection")
            };
            roots.push(ty.clone());
        }
        roots.push(self.intermediate().clone());
        roots.push(self.output().clone());
        roots
    }
    pub(crate) fn intermediate(&self) -> &FunctionValueType {
        &self
            .selected
            .selected
            .aggregate
            .as_ref()
            .unwrap()
            .intermediate_type
    }
    pub(crate) fn output(&self) -> &FunctionValueType {
        let FunctionResultType::Scalar(ty) = &self.selected.selected.result_type else {
            panic!("scalar aggregate result")
        };
        ty
    }
    pub(crate) fn signature(&self, request_id: u32) -> plan::ResolvedAggregateSignature {
        let aggregate = self.selected.selected.aggregate.as_ref().unwrap();
        plan::ResolvedAggregateSignature {
            writer_preparation_call: None,
            function_id: self.selected.function_id.as_str().into(),
            logical_argument_count: Some(u32::try_from(self.logical_count).unwrap()),
            state_argument_contract:
                novarocks_plan_codec::physical_aggregate_binding_v2::encode_state_argument_contract(
                    aggregate.state_argument_contract,
                ),
            original_request_id: Some(request_id),
            overload_identity: self.selected.selected.overload.as_str().into(),
            argument_types: self
                .selected
                .selected
                .argument_types
                .iter()
                .map(|arg| {
                    let FunctionArgumentType::Value(ty) = arg else {
                        panic!("value selection")
                    };
                    sources::type_desc_with_nullable(ty, ty.nullable())
                })
                .collect(),
            intermediate_type: Some(sources::type_desc_with_nullable(
                self.intermediate(),
                self.intermediate().nullable(),
            )),
            output_type: Some(sources::type_desc_with_nullable(
                self.output(),
                self.output().nullable(),
            )),
            state_format_identity: aggregate.state_format.as_str().into(),
        }
    }
    pub(crate) fn request(
        &self,
        request_id: u32,
        definition: wire::CallRequestDefinition,
    ) -> wire::OriginalCallRequest {
        let p = policy();
        wire::OriginalCallRequest {
            request_id: Some(request_id),
            definition: Some(definition),
            arguments: self
                .arguments
                .iter()
                .zip(&self.references)
                .map(|(arg, constant)| {
                    let FunctionArgument::Value { value_type, .. } = arg else {
                        panic!("value source")
                    };
                    wire::OriginalFunctionArgument {
                        kind: Some(wire::original_function_argument::Kind::Value(
                            wire::OriginalValueArgument {
                                value_type_id: sources::type_desc_with_nullable(
                                    value_type,
                                    value_type.nullable(),
                                )
                                .value_binding_id,
                                constant: constant.clone(),
                            },
                        )),
                    }
                })
                .collect(),
            logical_argument_count: Some(u32::try_from(self.logical_count).unwrap()),
            expected_result_value_type_id: None,
            constant_policy: Some(wire::SourceConstantPolicy {
                max_rows: Some(p.max_rows),
                max_array_nodes: Some(p.max_array_nodes),
                max_logical_elements: Some(p.max_logical_elements),
                max_retained_buffer_bytes: Some(p.max_retained_buffer_bytes),
                max_type_depth: Some(p.max_type_depth),
                max_type_nodes: Some(p.max_type_nodes),
                max_dictionary_depth: Some(p.max_dictionary_depth),
                max_metadata_bytes: Some(p.max_metadata_bytes),
                max_library_validation_work: Some(p.max_library_validation_work),
                max_library_validation_bytes: Some(p.max_library_validation_bytes),
            }),
        }
    }
}
pub(crate) fn aggregate_definition(node: i32, call: u32) -> wire::CallRequestDefinition {
    wire::CallRequestDefinition {
        kind: Some(wire::call_request_definition::Kind::Relational(
            semantics::CallSite {
                kind: Some(semantics::call_site::Kind::Aggregate(
                    semantics::NodeCallSite {
                        node_id: Some(u32::try_from(node).unwrap()),
                        call,
                    },
                )),
            },
        )),
    }
}
pub(crate) fn source_catalogue() -> Vec<AggregateSource> {
    let mut sources = vec![
        AggregateSource::new("sum", &[DataType::Int64], &[DataType::Int64]),
        AggregateSource::new("sum", &[DataType::Int32], &[DataType::Int32]),
        AggregateSource::new(
            "max_by",
            &[DataType::Utf8, DataType::Int64],
            &[DataType::Utf8, DataType::Int64],
        ),
        AggregateSource::new(
            "min_by",
            &[DataType::Utf8, DataType::Int64],
            &[DataType::Utf8, DataType::Int64],
        ),
        AggregateSource::new(
            "array_agg",
            &[DataType::Utf8],
            &[DataType::Utf8, DataType::Int64],
        ),
        AggregateSource::new("avg", &[DataType::Int64], &[DataType::Int64]),
        AggregateSource::new("count", &[], &[]),
        AggregateSource::new("count", &[DataType::Int64], &[DataType::Int64]),
        AggregateSource::with_literals(
            "count_if",
            &[DataType::Boolean],
            &[DataType::Boolean],
            &[(0, common::literal_value::Value::BoolValue(true))],
        ),
    ];
    for name in ["group_concat", "string_agg"] {
        for update in [
            vec![DataType::Utf8, DataType::Utf8],
            vec![DataType::Utf8, DataType::Utf8, DataType::Int64],
        ] {
            sources.push(AggregateSource::with_literals(
                name,
                &[DataType::Utf8, DataType::Utf8],
                &update,
                &[(1, common::literal_value::Value::StringValue("|".into()))],
            ));
        }
    }
    sources
}
